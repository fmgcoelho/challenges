use std::collections::HashMap;

use anyhow::{
    Result,
    anyhow
};

use polars::{self, frame::DataFrame, io::SerReader, prelude::{col, CsvReadOptions, IntoLazy, PlSmallStr}};
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
            println!("Fail capture{c:#?}");
        }
    }
    result
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
    println!("A column {:#?}", df.column(&grade_cols[0]));
    Ok(())
}