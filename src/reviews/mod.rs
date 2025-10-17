use std::collections::{HashMap, HashSet};
use std::fs::{remove_file, File};
use std::io::{BufReader, Write};
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Result, anyhow};

use convert_case::{Case, Casing};
use handlebars::{Context, Handlebars, Helper, HelperResult, Output, RenderContext};
use polars::prelude::*;
use polars::{self, frame::DataFrame, io::SerReader};
use regex::Regex;
use serde_json::{Value, from_reader, json};

enum ReviewPart {
    Feedback()
}
struct Review {
    answer_id: usize,
    reviewer: String,
    author: String,
    raw_reviews:  
}

fn split_column_names(df: &DataFrame) -> HashMap<String, (usize, String, String)> {
    let cols = df.get_column_names();
    let mut result = HashMap::<String, (usize, String, String)>::new();
    let names_re = Regex::new(r"\((?<label>[^\)]+)\)(?<text>.+)").unwrap();
    let spaces_re = Regex::new(r"   +").unwrap();

    for (i, column_name) in cols.iter().enumerate() {
        if let Some(caps) = names_re.captures(column_name) {
            let label = caps["label"].to_string();
            let text: String = spaces_re.replace(caps["text"].trim(), " - ").to_string();
            result.insert(
                column_name.to_string(),
                (i, label.clone(), text.clone()));
        } else {
            println!("Failed capture \"{names_re:#?}\" in \"{column_name:#?}\".");
        }
    }
    result
}

fn score(s: &str) -> f32 {
    if s.starts_with("YES") {
        1.0
    } else if s.starts_with("NO") {
        0.0
    } else {
        0.5
    }
}

fn str_to_score(str_val: &Column) -> Column {
    str_val
        .str()
        .unwrap()
        .into_iter()
        .map(|opt_name: Option<&str>| opt_name.map(|name: &str| score(name)))
        .collect::<Float32Chunked>()
        .into_column()
}

pub fn process(
    df: &DataFrame,
    gradding_cols: &[String],
    feedback_cols: &[String],
) -> Result<DataFrame> {
    // Get all the authors
    let authors: HashSet<String> = df
        .column("author")?
        .str()?
        .into_iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();

    // Get all the reviewers
    let reviewers: HashSet<String> = df
        .column("reviewer")?
        .str()?
        .into_iter()
        .flatten()
        .map(|s| s.to_string())
        .collect();

    //
    //  Result Dataframe columns:
    //
    //  student [q-autgrade q-revgrade for q in gradding_cols] [q for q in feedback-cols]
    //
    let mut result = df!(
        "student" => authors.iter().map(|s| s.to_string()).collect::<Vec<String>>(),
    )?;

    //
    //  Author grades
    //
    // 1. Build aggregation expression: [col(q), col(q).mean().alias(q-autgrade) for q in gradding_cols]
    // 2. Group by author: df.group_by "author" and aggregate
    // 3. Drop irrelevant columns and join with result
    //
    let mut mean_agg: Vec<Expr> = vec![];
    for gc in gradding_cols {
        let mean_expr = col(gc).mean().alias(format!("{gc}-autgrade"));
        mean_agg.push(col(gc));
        mean_agg.push(mean_expr);
    }
    let author_grades = df
        .clone()
        .lazy()
        .group_by([col("author")]) // eg: q = [rg1 , rg2, rg3 ]
        .agg(mean_agg) // eg: q-autgrade = x
        .collect()? // lazy => eager
        .drop_many(gradding_cols); // cols = q-autgrade for q ...
    // Add author_grades to result
    result = result.join(
        &author_grades,
        ["student"],
        ["author"],
        JoinArgs::new(JoinType::Left),
        None,
    )?;

    /*
    Iterate over:
    authors
        gradding_cols
            agrade = results[ [author, col]  ]
            reviewers
                ar_grade = df [[author, reviewer], col]
                revgrad[reviewer] = 1 - | agrade - ar_grade |
     */
    let mut reviewer_grades = df!(
        "student" => authors.iter().map(|s| s.to_string()).collect::<Vec<String>>(),
    )?;
    for author in authors.iter() {
        let author_mask = result.column("student")?.str()?.contains(author, true)?;
        let grades = author_grades.filter(&author_mask)?;
        for gc in gradding_cols.iter() {
            let agrade = grades.column(&format!("{gc}-autgrade"))?.get(0)?;
            let agrade: f32 = agrade.try_extract()?;
            // println!("Grade of question {gc:^30} for {author:>6}: {agrade:>4.2}");

            let gc_revgrade = format!("{gc}-revgrade");
            reviewer_grades = reviewer_grades
                .clone()
                .lazy()
                .with_column(lit(0.0).alias(gc_revgrade.clone()))
                .collect()?;

            for reviewer in reviewers.iter() {
                // let ar_mask = ;
                let ar_grade = df
                    .clone()
                    .lazy()
                    .filter(
                        col("author")
                            .eq(lit(author.to_string()))
                            .and(col("reviewer").eq(lit(reviewer.to_string()))),
                    )
                    .collect()?;
                let ar_grade: Vec<f32> = ar_grade
                    .column(gc)?
                    .f32()?
                    .into_no_null_iter()
                    .collect::<Vec<_>>();
                if !ar_grade.is_empty() {
                    let ar_grade = ar_grade[0];

                    let r_grade = 1.0 - (agrade - ar_grade).abs();

                    let update_expr = when(col("student").eq(lit(reviewer.to_string())))
                        .then(lit(r_grade))
                        .otherwise(col(gc_revgrade.clone()))
                        .alias(gc_revgrade.clone());
                    reviewer_grades = reviewer_grades.lazy().with_column(update_expr).collect()?;
                    // println!("\tGrade by {reviewer:>6}: {ar_grade:>4.2};\tReviewer grade: {r_grade:>4.2}.");
                }
            }
        }
    }

    result = result.join(
        &reviewer_grades,
        ["student"],
        ["student"],
        JoinArgs::new(JoinType::Left),
        None,
    )?;

    // autgrade = mean(autgrade_cols)
    let autgrade_cols: Vec<Expr> = result
        .clone()
        .get_column_names_str()
        .iter()
        .filter(|c| c.ends_with("-autgrade"))
        .map(|c| col(c.to_string()))
        .collect();
    let autgrade = result
        .clone()
        .lazy()
        .select(&autgrade_cols)
        .collect()?
        .mean_horizontal(NullStrategy::Ignore)?
        .unwrap()
        .with_name("autgrade".to_string().into());

    // revgrade = mean(revgrade_cols)
    let revgrade_cols: Vec<Expr> = result
        .clone()
        .get_column_names_str()
        .iter()
        .filter(|c| c.ends_with("-revgrade"))
        .map(|c| col(c.to_string()))
        .collect();
    let revgrade = result
        .clone()
        .lazy()
        .select(&revgrade_cols)
        .collect()?
        .mean_horizontal(NullStrategy::Ignore)?
        .unwrap()
        .with_name("revgrade".to_string().into());
    // add autgrade and revgrade cols
    result = result.hstack(&[autgrade, revgrade])?;

    // FEEDBACK
    let mut feedback_sel: Vec<Expr> = feedback_cols.iter().map(|c| col(c.to_string())).collect();
    feedback_sel.push(col("author"));

    let re_spaces = Regex::new(r"   +")?;
    let mut normalized_feedback = vec![];
    let mut fba_cols: Vec<String> = feedback_cols.to_vec();
    fba_cols.push("author".to_string());
    for c in df.columns(fba_cols)? {
        let l: Vec<String> = c
            .str()?
            .into_iter()
            .map(|opt| opt.unwrap_or("").to_string())
            .collect();
        let nc: Vec<String> = l
            .iter()
            .map(|s| re_spaces.replace(s.trim(), " -- ").to_string())
            .collect();
        normalized_feedback.push(Column::new(c.name().clone(), nc));
    }
    let feedback = DataFrame::new(normalized_feedback)?;
    // println!("{feedback}");

    let feedback_aggr: Vec<Expr> = feedback_cols
        .iter()
        // .flat_map(|c| [col(c), col(c).sum().alias(format!("{c}-all"))])
        .map(col)
        .collect();
    let feedback = feedback
        .clone()
        .lazy()
        .group_by([col("author")])
        .agg(feedback_aggr)
        // .select(feedback_cols)
        .collect()?
        .lazy()
        .select(feedback_sel)
        .collect()?;

    result = result.join(
        &feedback,
        ["student"],
        ["author"],
        JoinArgs::new(JoinType::Left),
        None,
    )?;
    Ok(result)
}

const REPORT_HEAD: &str = r#"## Grades

| _Author_ | _Reviewer_ | **Total** |
|---------:|-----------:|----------:|
| {{pct autgrade}} | {{pct revgrade}} | {{pct totgrade}} |

### Detailed

| Question | _Author_ | _Reviewer_ |
|:---------|---------:|-----------:|
{{#each questions}}
| {{{this.questiontext}}} | {{pct this.autgrade}} | {{pct this.revgrade}} |
{{/each}}

### Feedback

{{#each feedback}}
#### {{this.questiontext}}

{{#each this.comments}}
{{{this}}}

----

{{/each}}
{{/each}}
"#;


// implement via bare function
fn pct_helper(
    h: &Helper,
    _: &Handlebars,
    _: &Context,
    _rc: &mut RenderContext,
    out: &mut dyn Output,
) -> HelperResult {
    let param = h.param(0).unwrap().value().as_f64().unwrap_or_default();
    let pfmt = format!("{:>3.0}%", param * 100.0);

    out.write(&pfmt)?;
    Ok(())
}

fn report(grades: &mut DataFrame, cinfo: &HashMap<usize, (String, String)>) -> Result<HashMap<String, String>> {
    let mut hb = Handlebars::new();
    hb.register_template_string("report", REPORT_HEAD)?;
    hb.register_helper("pct", Box::new(pct_helper));

    let mut file = File::create("temp.json")?;
    JsonWriter::new(&mut file)
        .with_json_format(JsonFormat::Json)
        .finish(grades)
        .unwrap();

    let file = File::open("temp.json")?;
    let reader = BufReader::new(file);
    // Read the JSON contents of the file as an instance of `User`.
    let u: Value = from_reader(reader)?;
    remove_file("temp.json")?;

    let mut reports = HashMap::new();
    if let Value::Array(students) = u {
        for mut student in students {
            if let Value::Object(ref mut st) = student {

                let mut qs: Vec<_> = vec![];
                let mut fbs: Vec<_> = vec![];

                for i in 0..cinfo.len() {
                    let (qlabel, qtext) = cinfo.get(&i).unwrap();
                    if qlabel.ends_with("-feedback") {
                        let title = qlabel.replace("-", " ").to_case(Case::Title);
                        let d = HashMap::from([
                            ("questiontext", Value::String(title)),
                            ("comments", st.get(qlabel).unwrap().clone()),
                        ]);
                        fbs.push(json!(d));
                    } else {
                        let autgrade = st.get(&format!("{qlabel}-autgrade"));
                        let revgrade = st.get(&format!("{qlabel}-revgrade"));
                        if autgrade.is_some() {
                            qs.push(
                                json!(HashMap::from([
                                    ("questiontext", Value::String(qtext.clone())),
                                    ("autgrade", autgrade.unwrap().clone()),
                                    ("revgrade", revgrade.unwrap().clone()),
                                ])));
                        }
                    }
                  }

                let student_id = st.get("student").unwrap().as_str().unwrap();
                let ag = st.get("autgrade").unwrap().as_f64().unwrap();
                let rg = st.get("revgrade").unwrap().as_f64().unwrap();
                let d = HashMap::from([
                    ("student", st.get("student").unwrap().clone()),
                    ("autgrade", st.get("autgrade").unwrap().clone()),
                    ("revgrade", st.get("revgrade").unwrap().clone()),
                    ("totgrade", json!((ag + rg) / 2.0)),
                    ("questions", json!(qs)),
                    ("feedback", json!(fbs)),
                    ]);
                let student_report = hb.render("report", &d)?;
                reports.insert(student_id.to_string(), student_report.to_string());
            }
        }
    }
    Ok(reports)
}

/*
    1. Read source to `df` (DataFrame)
    2. Normalize columns names:
        - `column_name` | match `\(label\)text` .| `{ column_name: (label, text)}`
        - `df.columns` | rename `column_name: label`
    3. Categorize columns:
        - `all_cols`: All column names in `df`;
        - `feedback_cols`: col name ends in `"feedback"`
        - `ident_cols`: `["author", "reviewer"]`
        - `gradding_cols`: `all_cols - (ident_cols + ident_cols)`
    4. Fill missing values: `null` -> `0.0`
    5. Transform `Y/N/M` scores into `1.0/0.0/0.5` values
*/
pub fn grade_projects(source: &str, count: usize) -> Result<()> {
    println!("Going to grade the reviews in {source} with {count} projects.");

    //
    //  1. Read source to `df` (DataFrame)
    //
    let mut df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(source.into()))?
        .finish()?;
    /*
    2. Normalize columns names:
        - `column_name` | match `\(label\)text` .| `{ column_name: (label, text)}`
        - `df.columns` | rename `column_name: label`
     */
    let cols_info = split_column_names(&df);
    // Rename (normalize) the column names to column label
    let dfc = df.clone();
    let column_names = dfc.get_column_names();
    for col in column_names {
        if let Some((_, name, _)) = cols_info.get(col.as_str()) {
            df.rename(col, PlSmallStr::from_str(name))?;
        } else {
            return Err(anyhow!("Failed normalizing column name: \"{col}\"."));
        }
    }
    /*
    3. Categorize columns:
        - `all_cols`: All column names in `df`;
        - `feedback_cols`: col name ends in `"feedback"`
        - `ident_cols`: `["author", "reviewer"]`
        - `gradding_cols`: `all_cols - (ident_cols + ident_cols)`
     */
    let all_cols: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    //
    //  Feedback columns
    //
    let feedback_cols: Vec<String> = all_cols
        .iter()
        .filter(|n| n.contains("feedback"))
        .map(|c| c.to_string())
        .collect();
    //
    //  Lists of `authors` and `reviewers`
    //
    let ident_cols: Vec<String> = vec!["reviewer".to_string(), "author".to_string()];
    //
    //  Grade columns
    //
    let gradding_cols: Vec<String> = all_cols
        .iter()
        .filter(|n| {
            let ns = n.to_string();
            !feedback_cols.contains(&ns) && !ident_cols.contains(&ns) && ns != "answer_id"
        })
        .map(|c| c.to_string())
        .collect();
    /*
    4. Fill missing values: `null` -> `0.5`
     */
    for c in gradding_cols.iter() {
        df.clone()
            .lazy()
            .with_column(col(c).fill_null(0.5))
            .collect()?;
    }
    /*
    5. Transform `Y/N/M` scores into `1.0/0.0/0.5` values
     */
    for col in gradding_cols.iter() {
        df.apply(col, str_to_score)?;
    }
    let mut results = process(&df, &gradding_cols, &feedback_cols)?;
    let cols_map: HashMap<_, _> = cols_info
        .values()
        .map(|(i, a, b)| (*i, (a.to_string(), b.to_string())))
        .collect();
    let reports_md = report(&mut results, &cols_map)?;

    let path = std::path::Path::new(source);
    let parent = path.parent().unwrap_or(std::path::Path::new("."));
    let mut collected_reports = Vec::<String>::new();
    for (student_id, report) in reports_md.iter() {
        let mut path_md = PathBuf::new(); 
        path_md.push(parent);
        path_md.push(student_id);
        path_md.set_extension("md");

        let mut file = File::create(&path_md)?;
        file.write_all(report.as_bytes())?;

        /*
        pandoc {{basename}}.md -f gfm -t html5 --standalone --metadata title=\"Challenge Results\" -o ${{basename}}.html
         */
        let path_html = path_md.with_extension("html");
        let _ = Command::new("pandoc")
            .args([
                "-f", "gfm", 
                "-t", "html5", 
                "--standalone", 
                "--metadata", &format!("title={student_id} Results"),
                "-o", path_html.to_str().unwrap(),
                path_md.to_str().unwrap()])
            .output()?;
        collected_reports.push(student_id.to_string());
    }

    let index = r#"# Reports

| Student |
|:-------:|
{{#each this}}
| [{{this}}]({{this}}.html) |
{{/each}}
    "#;
    let mut hb = handlebars::Handlebars::new();
    hb.register_template_string("index", index)?;
    let mut pb = PathBuf::new();
    pb.push(parent);
    pb.push("index");
    pb.with_extension("md");
    let mut file = File::create(&pb)?;
    let index_md = hb.render("index", &collected_reports)?;
    file.write_all(index_md.as_bytes())?;
    let _ = Command::new("pandoc")
        .args([
            "-f", "gfm", 
            "-t", "html5", 
            "--standalone", 
            "--metadata", "title=Challenge Results",
            "-o", pb.with_extension("html").to_str().unwrap(),
            pb.to_str().unwrap()])
        .output()?;

    println!("OK");
    Ok(())
}
