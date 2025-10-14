use std::collections::{HashMap, HashSet};
use std::fmt::format;

use anyhow::{
    Result,
    anyhow
};

use polars::chunked_array::collect;
use polars::{self, frame::DataFrame, io::SerReader};
use polars::prelude::*;
use regex::Regex;

fn split_column_names(df: &DataFrame) -> HashMap<String, (String,String)> {
    let cols = df.get_column_names();
    let mut result = HashMap::<String,(String, String)>::new();
    let re = Regex::new(r"\((?<label>[^\)]+)\)(?<fulltext>.+)").unwrap();

    for c in cols.iter() {
        if let Some(caps) = re.captures(c) {
            let label = caps["label"].to_string();
            let fulltext: String = caps["fulltext"].trim().to_string();
            result.insert(c.to_string(), (label.clone(), fulltext.clone()));
        } else {
            println!("Fail capture in {c:#?}");
        }
    }
    result
}

fn score(s: &str) -> f32 {
    if s.starts_with("YES") { 1.0 }
    else if s.starts_with("NO") { 0.0 }
    else { 0.5 }
}


fn str_to_score(str_val: &Column) -> Column {
    str_val.str()
        .unwrap()
        .into_iter()
        .map(|opt_name: Option<&str>| {
            opt_name.map(|name: &str| score(name))
         })
        .collect::<Float32Chunked>()
        .into_column()
}

pub fn process(df: &DataFrame, gradding_cols: &Vec<String>, _feedback_cols: &Vec<String>) -> Result<()> {

    let mut mean_agg: Vec<Expr> = vec![];
    for gc in gradding_cols {
        let mean_expr = col(gc).mean().alias(format!("{gc}-authgrade"));
        mean_agg.push(col(gc));
        mean_agg.push(mean_expr);
    }

    let author_groups = df.clone().lazy()
        .group_by([col("author")]);
    let author_grades = author_groups
        .agg(mean_agg).collect()?;
    let author_grades = author_grades.drop_many(gradding_cols);
    
    let reviewers: HashSet<String> = df
        .column("reviewer")?
        .str()?
        .into_iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();
    
    let authors: HashSet<String> = author_grades
        .column("author")?
        .str()?
        .into_iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();


    let result = df.clone().lazy();
    for gc in gradding_cols.iter() {
        result.with_column(lit(0.0).alias(format!("{gc}-revgrade")));
    }
    for author in authors.iter() {
        let author_mask = author_grades.column("author")?.str()?.contains(author,true)?;
        let grades = author_grades.filter(&author_mask)?;
        for gc in gradding_cols.iter() {
            let agrade = grades.column(&format!("{gc}-authgrade"))?.get(0)?;
            let agrade:f32 = agrade.try_extract()?;
            println!("Grade of question {gc:^30} for {author:>6}: {agrade:>4.2}");

            for reviewer in reviewers.iter() {
                // let ar_mask = ;
                let ar_grade = df.clone().lazy()
                    .filter(
                    col("author").eq(lit(author.to_string()))
                    .and(
                        col("reviewer").eq(lit(reviewer.to_string()))
                    ))
                    .collect()?;
                let ar_grade: Vec<f32> = ar_grade.column(gc)?
                    .f32()?
                    .into_no_null_iter()
                    .collect::<Vec<_>>();
                if !ar_grade.is_empty() {
                    let ar_grade = ar_grade[0];

                    let r_grade = 1.0 - (agrade - ar_grade).abs();
                    println!("\tGrade by {reviewer:>6}: {ar_grade:>4.2};\tReviewer grade: {r_grade:>4.2}.");
                }
            }
        }
    }
    Ok(())
}

pub fn grade_projects(source: &str, count: usize) -> Result<()> {
    println!("Going to grade the reviews in {source} with {count} projects.");

    //
    //  Load CSV file into a DataFrame
    //
    let mut df = CsvReadOptions::default()
    .try_into_reader_with_file_path(Some(source.into()))?
    .finish()?;
    //
    //  Normalize -by label- the columns names; keep the original text
    //
    let cols_info = split_column_names(&df);
    // Rename (normalize) the column names to column label
    let dfc = df.clone();
    let column_names = dfc.get_column_names();
    for c in column_names {
        if let Some((n, _)) = cols_info.get(c.as_str()) {
            df.rename(c, PlSmallStr::from_str(n))?;
        } else {
            return Err(anyhow!("Failed normalizing column name: \"{c}\"."));
        }
    }
    //
    //  Fill missing values
    //
    let column_names = df.get_column_names_str();
    for c in column_names {
        df.clone().lazy()
            .with_column(col(c).fill_null(0.0))
            .collect()?;
    }
    //
    //  All columns
    //
    let all_columns = df.get_column_names();
    //
    //  Feedback columns
    //
    let feedback_cols:Vec<String> = all_columns.iter()
        .filter(|n| n.contains("feedback"))
        .map(|c| c.to_string()).collect();
    //
    //  Ident columns
    //
    let ident_cols:Vec<String> = vec![ 
        "reviewer".to_string(),        
        "author".to_string(), ];
    //
    //  Grade columns
    //
    let grade_cols:Vec<String> = all_columns.iter()
        .filter(|n| {
            let ns = n.to_string();
            !feedback_cols.contains(&ns) && !ident_cols.contains(&ns) && ns != "answer_id"})
        .map(|c| c.to_string()).collect();
    // println!("Grade cols: {:#?}", grade_cols);
    // println!("A column {:#?}", df.column(&grade_cols[0]));
    //
    // Score grade columns
    //
    for column in grade_cols.iter() {
        df.apply(column, str_to_score)?;
    }

    process(&df, &grade_cols, &feedback_cols)?;
    Ok(())
}