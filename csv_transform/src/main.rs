use anyhow::{bail, Result};
use clap::Parser;
use csv::Reader;
use sqlx::sqlite::SqliteRow;
use sqlx::Sqlite;
use sqlx::{Row, SqlitePool};
use std::io::prelude::*;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Use this cli to run sqlite queries to transform a csv into a new csv.",
    long_about = r#"
Use this cli to run sql queries to transform an in file into an outfile.
Runs on sqlite, you can refer to standard sqlite docs for query rules.
example usage with a csv at ./thiscsv.csv

csv_transform -f thiscsv.csv "select * from table1 where id = '235'" out.csv

the output argument is optional, if skipped will print to stdout

the initial table created from the csv will be named 'table1' and additional tables
will be named in order that they were given to this cli -> table2, table3 etc

the out.csv is a user defined file path which will be generated from the final select query
    "#
)]
struct Cli {
    /// the intake csv to transform
    #[arg(short, long)]
    fileinput: Vec<String>,
    /// the sqlite query used to transform the data
    sql: String,
    /// the file the final queries result will be outputted to
    outcsv: Option<String>,
}

async fn insert_data_helper(
    sql_string: &str,
    values: &[String],
    pool: &sqlx::Pool<Sqlite>,
) -> Result<()> {
    // println!("insert: {} values: {}", sql_string, values.join(" ->> "));
    let mut qb = sqlx::query(sql_string);
    for value in values.iter() {
        qb = qb.bind(value);
    }
    qb.execute(pool).await?;
    Ok(())
}

async fn insert_data(
    pool: &sqlx::Pool<Sqlite>,
    mut reader: Reader<std::fs::File>,
    tablename: String,
    headers: Vec<String>,
) -> Result<()> {
    let mut sql = vec!["INSERT INTO ".to_string(), tablename, "(".to_string()];
    sql.push(headers.join(","));
    sql.push(")VALUES".to_string());
    let insert_starter = sql.join("");
    sql.clear();

    for _ in 0..headers.len() {
        sql.push("?".to_string());
    }
    let placeholders = format!("({})", sql.join(","));
    sql.clear();
    let mut values = Vec::<String>::new();

    static INS_MAX: i64 = 300;
    let mut i = 0;
    for line in reader.records() {
        i += 1;
        sql.push(placeholders.clone());
        let mut bindable: Vec<String> = line?.deserialize(None)?;
        values.append(&mut bindable);
        if i >= INS_MAX {
            let sql_string = format!("{}{}", insert_starter, sql.join(","));
            insert_data_helper(&sql_string, &values, pool).await?;
            sql.clear();
            values.clear();
        }
    }
    if sql.is_empty() {
        return Ok(());
    }
    let sql_string = format!("{}{}", insert_starter, sql.join(","));
    insert_data_helper(&sql_string, &values, pool).await?;
    Ok(())
}

fn read_csv(cli: Vec<String>) -> Result<(String, Vec<String>, Reader<std::fs::File>)> {
    let input = std::fs::File::open(cli.into_iter().next().unwrap())?;
    let mut reader = Reader::from_reader(input);
    let header: Vec<String> = reader.headers().unwrap().deserialize(None)?;
    if header.is_empty() {
        bail!("empty csv given");
    }
    let mut sql = vec![
        r#"
create table table1 
(         
        "#
        .to_string(),
        header.join(
            r#" STRING NOT NULL DEFAULT '', 
        "#,
        ),
    ];
    sql.push(" STRING NOT NULL DEFAULT '' ".to_string());
    sql.push(")".to_string());
    Ok((sql.join(" "), header, reader))
}

fn read_queried_data(run_q: &[SqliteRow]) -> Result<String> {
    let mut results = Vec::<String>::new();
    let mut col_count = 0;
    for row in run_q {
        col_count = row.len();
        let mut intermediary = Vec::<String>::new();
        for i in 0..col_count {
            let v8: Vec<u8> = row.try_get_unchecked(i)?;
            let mut unescaped_str: String = std::str::from_utf8(&v8)?.to_string();
            if unescaped_str.find(',').is_some() {
                unescaped_str = format!("\"{}\"", unescaped_str);
            }
            intermediary.push(unescaped_str);
        }
        results.push(intermediary.join(","));
        results.push("\n".to_string());
    }

    Ok(format!(
        "{}\n{}",
        (1..col_count + 1)
            .map(|x| { format!("col{}", x) })
            .collect::<Vec<String>>()
            .join(","),
        results.join("")
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let pool = SqlitePool::connect("sqlite::memory:").await?;
    let (sql, headers, reader) = read_csv(cli.fileinput)?;
    sqlx::query(&sql).execute(&pool).await?;
    insert_data(&pool, reader, "table1".to_string(), headers).await?;
    let run_q = sqlx::query(&cli.sql).fetch_all(&pool).await?;
    if run_q.is_empty() {
        bail!("no records returned for query:\n{}", cli.sql);
    }
    let to_write = read_queried_data(&run_q)?;

    match cli.outcsv {
        None => {
            println!("\nresults:\n\n{}", to_write);
        }
        Some(outcsv) => {
            let mut writer = std::fs::File::create(outcsv)?;
            writer.write_all(to_write.as_bytes())?;
        }
    }

    Ok(())
}
