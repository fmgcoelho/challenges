use std::collections::{HashMap, HashSet};
use std::{fs::File, io::Seek, sync::Arc};

use anyhow::Result;
use anyhow::anyhow;
use arrow::array::{Array, AsArray, RecordBatch, StringArray};
use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use regex::Regex;

fn score(s: &str) -> f32 {
    if s.starts_with("YES") { 1.0 }
    else if s.starts_with("NO") { 0.0 }
    else { 0.5 }
}


// pub fn process(df: &DataFrame, gradding_cols: &Vec<String>, _feedback_cols: &Vec<String>) -> Result<()> {
//     Ok(())
// }

fn column(batch: &RecordBatch, name: &str) -> Option<Arc<dyn Array>> {
    batch.schema().column_with_name(name).map(|(index, _)| batch.column(index)).cloned()
}

fn rename_columns(batch: &RecordBatch) -> Result<(RecordBatch, HashMap<String, String>)> {

    // 1. Split the original column names "(name)text" into (name, text)
    let longname_re = Regex::new(r"^\((?<name>[^\)]+)\)(?<text>.*)$")?;
    let spaces_re = Regex::new(r"(   +)")?;
    let split_names: Vec<(String, String)> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if let Some(caps) = longname_re.captures(field.name()) {
                (caps["name"].to_string(), spaces_re.replace_all(caps["text"].trim(), " -- ").to_string())
            } else {
                ("".to_string(), "".to_string())
            } })
        .collect();
    let new_names: Vec<String> = split_names.iter().map(|(a,_)| a.clone()).collect();
    let split_names: HashMap<_, _> = split_names.iter().map(|(a,b)| (a.clone(), b.clone())).collect();

    // 2. Create new fields with the new names
    let new_fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .zip(new_names.iter())
        .map(|(field, name)| Field::new(
            name, 
            field.data_type().clone(), 
            field.is_nullable()))
        .collect();

    // 3. Create the new schema
    let schema = Schema::new(new_fields);

    // 4. Rebuild the RecordBatch
    let new_batch = RecordBatch::try_new(
        Arc::new(schema), 
        batch.columns().to_vec())?;

    Ok((new_batch, split_names))

}

fn grades2number(batch: &RecordBatch, gradding_cols: Vec<String>) -> Result<RecordBatch> {
    let result: &mut RecordBatch = &mut batch.clone();
    for gcol in gradding_cols {
        let c0: StringArray = column(result, &gcol)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        let c1: Vec<f32> = c0
            .iter()
            .map(|s| score(s.unwrap_or("NO")) )
            .collect();

        let current_schema = result
            .schema()
            .fields()
            .to_vec();
        let gcol_index = result
            .schema()
            .column_with_name(&gcol)
            .unwrap();
        let next_schema:Vec<Arc<Field>> = result
            .schema()
            .fields()
            .iter()
            .map(|c| if *c.name() == gcol { 
                Arc::new(Field::new(
                    gcol.to_string(),
                    DataType::Float32,
                    false))} else {c.clone()})  
            .collect();


        println!("{c1:#?}");
    }

    Err(anyhow!(""))
}

pub fn grade_projects(source: &str, count: usize) -> Result<()> {
    println!("Going to grade the reviews in {source} with {count} projects.");

    //
    //  Load CSV file into a DataFrame
    //
    let mut file = File::open(source)?;
    let format = Format::default().with_header(true);
    let (schema, _) = format.infer_schema(&mut file, Some(5))?;
    file.rewind()?;

    let builder = ReaderBuilder::new(Arc::new(schema)).with_format(format);
    let mut csv = builder.build(file)?;
    let batch = csv.next().unwrap()?; // A RecordBatch, in arrow language

    //
    //  Normalize columns names; keep the old text in `cols { name : text }`
    //
    let (batch, cols) = rename_columns(&batch)?;

    //
    //  Define columns with:
    //      - identities: `(author, reviewer)`
    //      - feedback: `*feedback`
    //      - gradding: otherwise
    //
    let id_cols = ["author".to_string(), "reviewer".to_string()];
    let feedback_cols: Vec<String> = cols
        .keys()
        .filter(|c| 
            c.ends_with("feedback"))
        .cloned()
        .collect();
    let gradding_cols: Vec<String> = cols
        .keys()
        .filter(|c|
            !id_cols.contains(c) && !feedback_cols.contains(c))
        .cloned()
        .collect();
    //
    //  Lists of `authors` and `reviewers`
    //
    let authors: HashSet<String> = column(&batch, "author")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .flatten()
        .map(|s| s.to_string())
        .collect::<HashSet<String>>();

    let reviewers: HashSet<String> = column(&batch, "reviewer")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .flatten()
        .map(|s| s.to_string())
        .collect::<HashSet<String>>();

    grades2number(&batch, gradding_cols);

    // println!("Authors: {:#?}\n\nReviewers: {:#?}", authors, reviewers);
    // println!("COLS: {cols:#?}");
    Ok(())
}