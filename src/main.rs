use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::Write;
use std::path::Path;

use clap::Parser;
use clap::ValueEnum;
use anyhow::Result;
use anyhow::anyhow;
use csv::Reader;
use rand::seq::IndexedRandom;
use rand::seq::SliceRandom;
use serde::Deserialize;


mod moodle_feedback;
mod reviews;


use crate::moodle_feedback::yaml2xml;
use crate::reviews::grade_projects;
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// What task to run.
    #[arg(value_enum)]
    task: Task,

    /// For tasks that require an input file.
    #[arg(short,long)]
    source: Option<String>,

    /// For tasks that rquire a number.
    #[arg(short, long)]
    count: Option<usize>,

}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum,Debug)]
enum Task {
    /// Randomly choose a (game) theme from a <source> file.
    PickTheme,
    /// Render a (Moodle's) Feedback XML from a <source> YAML file.
    RenderReviewerForm,
    /// Match <count> reviews for each student in <source> (CSV).
    PickReviews,
    /// Computes the AUTHOR and REVIEWER grades from <source>; Then renders a report in both markdown and html.
    GradeReviews,
}

fn pick_theme(file_path: &str) -> Result<String> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let lines: Vec<String> = reader.lines().collect::<Result<_, _>>()?;
    
    if let Some(random_line) = lines.choose(&mut rand::rng()) {
        Ok(random_line.to_string())
    } else {
        Err(anyhow!("The file is empty."))
    }
} 

#[derive(Debug, Deserialize)]
struct AuthorsLine {
    #[serde(rename = "Author")]
    author: String,
    #[serde(rename = "StatusOK")]
    status_ok: bool
}

fn pick_reviews(file_path: &str, num_reviews: usize) -> Result<String> {


    // Read the CSV file
    let mut reader = Reader::from_path(file_path)?;
    
    // Retain only the well-formed and "StatusOk" records
    let mut authors = Vec::<String>::new();
    let iter = reader.deserialize();
    for result in iter {
        let record: AuthorsLine = result?;
        if record.status_ok {
            authors.push(record.author);
        }
    }

    // Extract the names
    authors.shuffle(&mut rand::rng());

    // Number of authors
    let authors_count = authors.len();

    // Sanity: the number of reviews assigned to each author must not be >= the number of authors
    let num_reviews_ok = if num_reviews >= authors_count-1 { authors_count - 1} else { num_reviews };

    // This simplies assignment
    authors.extend(authors.clone());

    // HashMap reviewer: authors
    let mut reviews = Vec::<Vec::<String>>::new();
    // Header
    let mut header = vec!["Reviewer".to_string()];
    let tail: Vec<String> = (1..(num_reviews_ok+1)).map(|i| format!("Author #{i}")).collect();
    header.extend_from_slice( &tail );
    reviews.push( header  );
    // LINE
    reviews.push((0..(num_reviews_ok+1)).map(|_| "-".to_string()).collect());
    // POPULATE
    for i in 1..authors_count {
        let targets = authors[i..(i+num_reviews_ok)].to_vec();
        let mut line = vec![authors[i].clone()];
        line.extend_from_slice(&targets);
        reviews.push( line );
    }

    // Generate the markdown table:
    let lines = reviews.iter().map(|v| format!("|{}|", v.join("|"))).collect::<Vec<_>>().join("\n");

    Ok(format!("# Reviews\n\n{lines}"))
}

fn main() -> Result<()>{
    let args = Args::parse();

    match &args.task {
        Task::PickTheme => {
            if let Some(source) = args.source {
                match pick_theme(&source) {
                    Ok(result) => println!("Theme: {result}"),
                    Err(e) => return Err(anyhow!("Can't pick a theme in {source}: {e}."))                    
                }
            } else {
                return Err(anyhow!("Missing theme source."));                
            }
        },
        Task::RenderReviewerForm => {
            if let Some(source) = args.source {
                match yaml2xml(&source) {
                    Ok(result) => {
                        let source_path = Path::new(&source);
                        let target_path = source_path.with_extension("xml");

                        let mut file = File::create(target_path)?;
                        file.write_all(result.as_bytes())?;
                        // println!("{result}");
                    },
                    Err(e) => return Err(anyhow!("{source} doesn't seem adequate: {e}."))                    
                }
            } else {
                return Err(anyhow!("Missing YAML source."));                
            }
        },
        Task::PickReviews => {
            if let Some(source) = args.source {
                let num_reviews: usize = args.count.unwrap_or(3);
                match pick_reviews(&source, num_reviews) {
                    Ok(result) => println!("{result}"),
                    Err(e) => return Err(anyhow!("Can't pick reviewers in {source}: {e}."))                    
                }
            } else {
                return Err(anyhow!("Missing reviewers source."));                
            }
        },
        Task::GradeReviews => {
            if let Some(source) = args.source {
                let num_reviews: usize = args.count.unwrap_or(3);
                match grade_projects(&source, num_reviews) {
                    Ok(()) => {},
                    Err(e) => return Err(anyhow!("Can't grade_projects in {source}: {e}."))                    
                }
            } else {
                return Err(anyhow!("Missing theme source."));                
            }
        },
    }
    Ok(())
}
