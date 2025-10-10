use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

use anyhow::Result;
use handlebars::Handlebars;
use serde::Deserialize;
use serde::Serialize;
use serde_yaml_ng as serde_yaml;

use markdown::{Options, to_html_with_options};

#[derive(Deserialize, Debug, Clone)]
struct YamlItem {
    #[serde(rename = "type")]
    item_type: String,
    label: Option<String>,
    text: Option<String>,
    // direction: Option<String>,
    options: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
struct YamlForm {
    items: Vec<YamlItem>,
}

#[derive(Debug, Default, Serialize)]
struct XmlItem {
    #[serde(rename = "@TYPE")]
    item_type: String,
    // required = required ? 1 : 0
    #[serde(rename = "@REQUIRED")]
    required: u8,

    // item_id: sequential!
    #[serde(rename = "ITEMID")]
    item_id: usize,

    /*
        itemtext:
        - html if type is textfield, textarea, multichoice
        - empty otherwise
    */
    #[serde(rename = "ITEMTEXT")]
    item_text: String,

    // itemlabel: unique except if type is "label"
    #[serde(rename = "ITEMLABEL")]
    item_label: String,

    /*
        presentation:
        - html if type is label
        - rows|cols if type is textfield
        - options format if type is multichoice:
            - d>>>>>OPTION
            - OPTION
            - concatenated by |\
        - empty otherwise
    */
    #[serde(rename = "PRESENTATION")]
    presentation: String,

    /*
        options:
        - h or v(?) if type is multichoice, menu
    */
    #[serde(rename = "OPTIONS")]
    options: String,
    // dependitem: 0
    #[serde(rename = "DEPENDITEM")]
    depend_item: u8,
    // dependvalue: ""
    #[serde(rename = "DEPENDVALUE")]
    depend_value: String,
}

fn md2html(md_text: &str) -> String {
    to_html_with_options(
        md_text,
        &Options::gfm()
        // &Options {
        //     parse: ParseOptions {
        //         constructs: Constructs {
        //             code_indented: false,
        //             character_escape: false,
        //             ..Constructs::default()
        //         },
        //         ..ParseOptions::default()
        //     },
        //     compile: CompileOptions {
        //         allow_dangerous_html: true,
        //         ..CompileOptions::gfm()
        //     },
        // },
    )
    .expect("Bad markdown in \"{md_text}\".")
}

impl From<YamlItem> for XmlItem {
    fn from(value: YamlItem) -> Self {
        let mut result = XmlItem {
            item_type: value.item_type.clone(),
            required: 1,
            item_id: 0,
            item_text: String::new(),
            item_label: value.label.clone().unwrap_or_default(),
            presentation: String::new(),
            options: String::new(),
            depend_item: 0,
            depend_value: String::default(),
        };

        match value.item_type.clone().as_str() {
            "mdarea" => {
                result.item_type = "label".to_string();
                result.required = 0;
                let mtext = value.text.clone().unwrap_or_default();
                result.presentation = md2html(&mtext);
            }
            "textfield" => {
                let mtext = value.text.clone().unwrap_or_default();
                result.item_text = md2html(&mtext);
                result.presentation = "30|255".to_string();
            }
            "pagebreak" => {
                result.required = 0;
                result.item_id = 0;
            }
            "singleanswer" => {
                result.item_type = "multichoice".to_string();
                let mtext = value.text.clone().unwrap_or_default();
                result.item_text = md2html(&mtext);
                let mut options: Vec<String> = value
                    .options
                    .unwrap_or_default()
                    .iter()
                    .map(|x| md2html(str::trim(x)))
                    .collect();
                if !options.is_empty() {
                    options[0] = format!("d>>>>>{}", options[0]);
                }
                result.presentation = options.join("\n\n|");
                result.options = "h".to_string();
            }
            "textarea" => {
                let mtext = value.text.clone().unwrap_or_default();
                result.item_text = md2html(&mtext);
                result.presentation = "40|5".to_string()
            }
            _ => {}
        }

        result
    }
}

impl XmlItem {

    fn xml(self: &XmlItem) -> String {
        let mut handlebars = Handlebars::new();

        // Register a template
        handlebars
            .register_template_string(
                "item",
                r#"<ITEM TYPE="{{type}}" REQUIRED="{{required}}"><ITEMID><![CDATA[{{item_id}}]]></ITEMID><ITEMTEXT><![CDATA[{{{item_text}}}]]></ITEMTEXT><ITEMLABEL><![CDATA[{{item_label}}]]></ITEMLABEL><PRESENTATION><![CDATA[{{{presentation}}}]]></PRESENTATION><OPTIONS><![CDATA[{{{options}}}]]></OPTIONS><DEPENDITEM><![CDATA[0]]></DEPENDITEM><DEPENDVALUE><![CDATA[]]></DEPENDVALUE></ITEM>"#,
            )
            .expect("Bad Template");

        let d: HashMap<String, String> = HashMap::from([
            ("type".to_string(), 
                self.item_type.clone()),
            ("required".to_string(), 
                if self.required == 0 { "0".to_string() } else { "1".to_string() }),
            ("item_id".to_string(), format!("{}", self.item_id)),
            ("item_text".to_string(), 
                self.item_text.clone()),
            ("item_label".to_string(), 
                self.item_label.clone()),
            ("presentation".to_string(), 
                self.presentation.clone()),
            ("options".to_string(), 
                self.options.clone()),
        ]);
        handlebars.render("item", &d).unwrap_or_default()
    }
}

#[derive(Debug, Serialize)]
struct XmlForm {
    #[serde(rename = "ITEM")]
    items: Vec<XmlItem>,
}
#[derive(Serialize)]
struct XmlVec {
    items: Vec<String>
}

impl XmlForm {
    fn xml(self: &XmlForm) -> String {
        let xmlvec = XmlVec { 
            items: self.items.iter().map(|x| x.xml()).collect()
        };

        let mut handlebars = Handlebars::new();
        handlebars
            .register_template_string(
                "doc",
                r#"<?xml version="1.0" encoding="UTF-8" ?>
<FEEDBACK COMMENT="XML-Importfile for mod/feedback" VERSION="200701"><ITEMS>{{#each items}}{{{this}}}{{/each}}</ITEMS></FEEDBACK>"#).expect("Bad Template");

        handlebars.render("doc", &xmlvec).unwrap_or_default()
    }
}

pub fn yaml2xml(source: &str) -> Result<String> {
    let file = File::open(source)?;
    let reader = BufReader::new(file);
    let yaml_doc: YamlForm = serde_yaml::from_reader(reader)?;
    let xml_items: Vec<XmlItem> = yaml_doc.items
        .into_iter()
        .map(XmlItem::from)
        .enumerate().map(|(i, mut x)| {
            x.item_id = i + 1;
            x
        })
        .collect();

    let xml_doc = XmlForm { items: xml_items };

    Ok(xml_doc.xml())
}
