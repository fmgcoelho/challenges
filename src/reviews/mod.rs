use std::{collections::{HashMap, HashSet}, f32, fs::File, io::Write, path::{Path, PathBuf}};

use anyhow::Result;
use anyhow::anyhow;
use convert_case::Casing;
use csv::StringRecord;

use handlebars::{Context, Handlebars, Helper, HelperResult, Output, RenderContext};

use log::{debug, error, trace};
use serde_json::json;

fn score_ynm(ynm: &str) -> f32 {
    if ynm.starts_with("Y") { 1.0 } 
    else if ynm.starts_with("N") { 0.0 } 
    else { 0.5 }
}

fn save_report(base_path: &Path, id: &str, text: &str) -> Result<()> {
    let mut path = PathBuf::new();
    path.push(base_path);
    path.push(id);
    path.set_extension("md");
    let mut file = File::create(&path)?;
    file.write_all(text.as_bytes())?;

    std::process::Command::new("pandoc")
    .args([
        "-f", "gfm",
        "-t", "html5", 
        "--standalone", 
        "-o", path.with_extension("html").to_str().unwrap(),
        path.to_str().unwrap()])
    .output()?;

    Ok(())
}

/// question_index => (label, text)
struct QuestionsInfo(HashMap<usize, (String, String)>);

fn pct (h: &Helper, _: &Handlebars, _: &Context, _: &mut RenderContext, out: &mut dyn Output) -> HelperResult {
    let param = h.param(0).unwrap();
    let p = if let Some(x) = param.value().as_f64() {
        x * 100.0
    } else {
        0.0
    };
    out.write(&format!("{p:>3.0}%"))?;
    Ok(())
}


const GRADE_REPORT: &str = r#"
# Report for student {{student}}

| _Total_ | Author |  Reviewer |
|--------:|-------:|----------:|
|{{pct totgrade}}|{{pct autgrade}}|{{pct revgrade}}|

## Detail

| _Question_ | Author |  Reviewer | 
|:-----------|-------:|----------:|
{{#each questions}}
|{{{this.text}}}|{{pct this.autgrade}}|{{pct this.revgrade}}|
{{/each}}

## Feedback

{{#each feedbacks}}
### {{this.label}}

{{#each this.feedback}}
{{{this}}}

---

{{/each}}
{{/each}}"#;

#[derive(Debug, Clone, Default)]
struct Grade {
    student: String,
    author_grades: HashMap<String, f32>,
    reviewer_grades: HashMap<String, f32>,
    feedbacks: HashMap<String, Vec<String>>, 
}

impl Grade {
    fn total_grades(&self) -> Result<(f32, f32)> {
        let num_questions = self.author_grades.len() as f32;
        let sum_ags: f32 = self.author_grades.values().sum();
        let sum_rgs: f32 = self.reviewer_grades.values().sum();
        Ok((sum_ags / num_questions, sum_rgs / num_questions))
    }

    fn report(&self, qinfo: &QuestionsInfo) -> Result<String> {
        let mut report_template = handlebars::Handlebars::new();
        report_template.register_template_string("student", GRADE_REPORT)?;
        report_template.register_helper("pct", Box::new(pct));

        let (autgrade, revgrade) = self.total_grades()?;
        let QuestionsInfo(q) = qinfo;
        let mut questions = vec![];
        let mut feedbacks = vec![];
        for i in 3..q.len() { // Skip if fields
            let (label, text) = q.get(&i).unwrap();
            if label.ends_with("feedback") {
                let fbs = self.feedbacks.get(label).unwrap().clone();
                feedbacks.push( json!({
                    "label": label.replace("-", " ").to_case(convert_case::Case::Title),
                    "feedback": fbs
                }))
            } else { // gradding question
                let agrade = self.author_grades.get(label).unwrap();
                let rgrade = self.reviewer_grades.get(label).unwrap();
                questions.push(json!({
                    "text": text.clone(),
                    "autgrade": agrade,
                    "revgrade": rgrade,
                }))
            }
        }
        let v = json!({
            "student": self.student,
            "totgrade": 0.5 *(autgrade + revgrade),
            "autgrade": autgrade,
            "revgrade": revgrade,
            "questions": questions,
            "feedbacks": feedbacks
        });        

        Ok(report_template.render("student", &v)?)
    }
}


const INDEX_REPORT: &str = r#"
# _"{{title}}"_

| Student Report |
|:--------------:|
{{#each report}}
| [{{this}}]({{this}}.html) |
{{/each}}
"#;

#[derive(Debug, Clone, Default)]
struct Grades(Vec<Grade>);

impl Grades {
    fn report(&self, cwd: &Path, qinfo: &QuestionsInfo, title: &str) -> Result<()>{
        // Collect all reports
        let reports = self.0.iter()
            .map(|g| (g.student.to_string(), g.report(qinfo).unwrap()))
            .collect::<HashMap<String, String>>();

        // Save reports in Markdown files
        for (id, md) in reports {
            save_report(cwd, &id, &md)?
        }

        // Create and save index 
        let mut students = self.students().iter().cloned().collect::<Vec<String>>();
        students.sort();
        let mut hb = handlebars::Handlebars::new();
        hb.register_template_string("index", INDEX_REPORT)?;
        let index_md = hb.render("index", &json!({
            "title": title,
            "report": &students}
        ))?;
        save_report(cwd, "index", &index_md)?;

        Ok(())
    }

    fn students(&self) -> HashSet<String> {
        self.0.iter().map(|g| g.student.clone()).collect()
    }

    fn grade_of(&self, student: &str) -> Option<Grade> {
        let opt = self.0.iter()
            .filter(|g| g.student == student)
            .map(|g| g.to_owned())
            .collect::<Vec<Grade>>();
        opt.first().cloned()
    }

    fn grade_reviewers(&self, reviews: &Reviews, qinfo: &QuestionsInfo, count: usize)  -> Result<Grades> {
        trace!("Start grade_reviews.");
        let mut grades: Vec<Grade> = vec![];
        for reviewer in self.students() {
            debug!("Gradding reviewer: {reviewer:?}.");
            // Initialize reviewer grades to 0.0
            let mut reviewer_grade = self.grade_of(&reviewer).unwrap();
            for (label,_) in qinfo.0.values() {
                reviewer_grade.reviewer_grades.insert(label.to_owned(), 0.0);
            }
            debug!("Reviewer initial grade: {reviewer_grade:#?}.");
            // println!("Current grade of {reviewer}:\n\t{:#?}.", reviewer_grade);
            for review in reviews.reviews_by(&reviewer) { // A review made by the reviewer
                let author = &review.author;
                // println!("Processing {:#?} of {author} by {reviewer}.", review);
                if let Some(author_grade) = self.grade_of(author) {
                    for (label, author_score) in author_grade.author_grades {
                        let reviewer_score = if let Some(given_score) = review.grades.get(&label) {
                            1.0 - (author_score - given_score).abs()
                        } else { 0.0 };

                        reviewer_grade.reviewer_grades.insert(
                            label.clone(),
                            if let Some(prev_grade) = reviewer_grade.reviewer_grades.get(&label) {
                                prev_grade + reviewer_score / (count as f32)
                            } else {
                                reviewer_score / (count as f32)
                            }
                        );
                    }
                } else {
                    let err = anyhow!("Author {author} has no grade.");
                    error!("{err:?}");
                    return Err(err)
                }
            }
            grades.push(reviewer_grade);
        }
        trace!("End grade_reviews.");
        Ok(Grades(grades))
    }
}

#[derive(Debug, Clone, Default)]
struct Review {
    reviewer: String,
    author: String,
    feedbacks: HashMap<String, String>,
    grades: HashMap<String, f32>
}

struct Reviews(Vec<Review>);

impl Reviews {
    fn authors(&self) -> HashSet<String> {
        self.0.iter().map(|r| r.author.clone()).collect()
    }

    fn reviews_of(&self, author: &str) -> Vec<Review> {
        self.0.iter().filter(|r| r.author == author).cloned().collect()
    }

    fn reviews_by(&self, reviewer: &str) -> Vec<Review> {
        self.0.iter().filter(|r| r.reviewer == reviewer).cloned().collect()
    }

    // fn reviewers(&self) -> HashSet<String> {
    //     self.0.iter().map(|r| r.reviewer.clone()).collect()
    // }

    // fn reviewers_of(&self, author: &str) -> Vec<String> {
    //     self.0.iter().filter(|r| r.author == author).map(|r| r.reviewer.clone()).collect()
    // }

    // fn authors_of(&self, reviewer: &str) -> Vec<String> {
    //     self.0.iter().filter(|r| r.reviewer == reviewer).map(|r| r.author.clone()).collect()
    // }

    // fn grade(&self, reviewer: &str, author: &str, label: &str) -> Option<f32> {
    //     self.0.iter()
    //         .filter(|r| r.reviewer == reviewer && r.author == author && r.grades.contains_key(label))
    //         .map(|r| r.grades[label])
    //         .collect::<Vec<f32>>()
    //         .get(0)
    //         .copied()
    // }

    fn grade_authors(&self) -> Result<Grades> {
        let mut gs = vec![];
        for author in self.authors() {
            let mut g = Grade { 
                student: author.clone(), 
                ..Default::default() };

            // grades: label: [g1, g2, ... ]
            let mut grades: HashMap<String, Vec<f32>> = HashMap::new();
            // let mut feedbs: HashMap<String, Vec<String>> = HashMap::new();
            for review in self.reviews_of(&author) {
                for (label, score) in review.grades {
                    let g_opt = grades.get_mut(&label.clone());
                    if g_opt.is_none() {
                        grades.insert(label.clone(), vec![]);
                    }
                    grades.get_mut(&label).unwrap().push(score);
                }
                for (label, feedb) in review.feedbacks {
                    let g_opt = g.feedbacks.get_mut(&label);
                    if g_opt.is_none() {
                        g.feedbacks.insert(label.clone(), vec![]);
                    }
                    g.feedbacks.get_mut(&label).unwrap().push(feedb.clone());
                }                
            }
            for (label, values) in &grades {
                let n: f32 = values.len() as f32;
                g.author_grades.insert(label.to_string(),                     
                    if n > 0.0 {
                        values.iter().sum::<f32>() / n}
                    else {0.0}
                );
            }
            gs.push(g);
        } 
        Ok(Grades(gs))
    }

    
    fn grades(&self, qinfo: &QuestionsInfo, count: usize) -> Result<Grades> {
        // Two steps:
        // 1. Reviews -> Grades (authors only)
        // 2. Grades (authors only) + Reviews -> Grades authors+reviewers
        let grades = self.grade_authors()?;
        grades.grade_reviewers(self, qinfo, count)
    }
}

fn normalize_headers(headers: &StringRecord) -> Result<QuestionsInfo> {
    let headers_re = regex::Regex::new(r"^\((?<label>[^\)]+)\)(?<text>.*)$")?;
    let spaces_re = regex::Regex::new(r"   +")?;
    let mut map: HashMap<usize, (String,String)> = HashMap::new();
    for (i, header) in headers.iter().enumerate() {
        if let Some(caps) = headers_re.captures(header) {
            let label = caps["label"].to_string();
            let text = caps["text"].trim().to_string();
            let text = spaces_re.replace(&text, " -- ").to_string();
            map.insert(i, (label.clone(), text));
        }
    }
    Ok(QuestionsInfo(map))
}

// Load reviews from CSV:
// id       columns: reviewer, author
// feedback columns: *feedback
// grading  columns: all the others
fn load_reviews(source: &str) -> Result<(QuestionsInfo, Reviews)> {

    let mut reader = csv::Reader::from_path(source)?;

    let hd_info = normalize_headers(reader.headers()?)?;
    let mut reviews:Vec<Review> = Vec::new();
    for record_result in reader.records() {
        let record = record_result?;
        let mut record_feedbacks = HashMap::new();
        let mut record_grades = HashMap::new();
        let mut record_reviewer = String::new();
        let mut record_author = String::new();
        for (i, r) in record.iter().enumerate() {
            match i {
                0 => record_reviewer = r.to_string(),
                1 => record_author = r.to_string(),
                _ => {
                    let QuestionsInfo(ref info) = hd_info; 
                    let (qlabel, _) = info.get(&i).unwrap(); 
                    let slabel = qlabel.to_string();
                    if slabel.ends_with("feedback") {
                        record_feedbacks.insert(slabel, r.to_string());
                    } else {
                        record_grades.insert(slabel, score_ynm(r));
                    }
                }
            }
        }
        reviews.push(
            Review { reviewer: record_reviewer, author: record_author, feedbacks: record_feedbacks, grades: record_grades }
        );
    }
    Ok((hd_info, Reviews(reviews)) )
}

pub fn grade_projects(source: &str, title: &str, count: usize) -> Result<()> {    

    let (qinfo, reviews) = load_reviews(source)?;

    let grades = reviews.grades(&qinfo, count)?;

    let cwd = if let Some(p) =  Path::new(source).parent() {
        p
    } else {
        Path::new(".")
    };
    grades.report(cwd, &qinfo, title)?;
    // println!("{:#?}", grades);
    Ok(())
}
