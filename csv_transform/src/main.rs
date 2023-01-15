use clap::Parser;
use std::io::{BufRead};
use tokio::{main, io::AsyncWriteExt};
use anyhow::{Result, bail};
use sqlx::{Value, ValueRef};
use csv::StringRecord;
use sqlx::{sqlite, SqlitePool, Row, decode};

#[derive(Parser, Debug)]
#[command(author, version, about = "Use this cli to run sqlite queries to transform a csv into a new csv.", 
    long_about = r#"
Use this cli to run sql queries to transform an in file into an outfile.
Runs on sqlite, you can refer to standard sqlite docs for query rules.
example usage with a csv at ./thiscsv.csv

csv_transform -f thiscsv.csv out.csv "select * from file where id = '235'"

the initial table created from the csv will be named 'table1' and additional tables
will be named in order that they were given to this cli -> table2, table3 etc

the out.csv is a user defined file path which will be generated from the final select query
    "#)]
struct Cli {
    /// the intake csv to transform
    #[arg(short, long)]
    fileinput: Vec<String>,
    /// the file the final queries result will be outputted to
    outcsv: String, 
    /// the sqlite query used to transform the data
    sql: String,
    
    /// default: false. If unnamed, the columns will be named as col1, col2, etc. and the first 
    /// row in the csv will be considered as the first row of data
    #[arg(short, long)]
    unnamed: Option<bool>
}

fn read_csv(cli: Vec<String>) -> Result<String> {
    let input = std::fs::File::open(cli.into_iter().next().unwrap())?;
    let mut reader = csv::Reader::from_reader(input);
    let header: Vec<String> = reader.headers().unwrap().deserialize(None)?;
    
    let mut records = Vec::<StringRecord>::new();
    for line in reader.records() {
        records.push(line?);
    }
    if records.len() < 1 {
        bail!("empty csv given");
    }
    let mut record_iter = records.iter();
    let _record: Vec<String> = record_iter.next().unwrap().deserialize(None)?;
    
    let mut sql = Vec::<String>::new();
    sql.push(r#"
create table table1 
(         
        "#.to_string());
    sql.push(header.join(r#" STRING NOT NULL DEFAULT '', 
        "#));
    sql.push(" STRING NOT NULL DEFAULT '' ".to_string());
    sql.push(")".to_string());
    let sql_string = sql.join(" ");
    Ok(sql_string)
}

#[main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let pool = SqlitePool::connect("sqlite::memory:").await?;
    let sql = read_csv(cli.fileinput)?;
    let ins_csv = sqlx::query(&sql)
        .execute(&pool)
        .await?;
    let mut run_q = sqlx::query("select * from table1")
        .fetch_all(&pool)
        .await?;
    if run_q.len() < 1 {
        bail!("no records returned for query:\n{}", cli.sql);
    }
    let col1: String = run_q.iter_mut().next().unwrap().try_get_raw(1)?.to_owned().try_decode()?;
    println!("{}", col1);
    Ok(())
}
