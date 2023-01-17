use anyhow::{bail, Result};
use clap::Parser;
use csv::{Reader, Writer};
use sqlx::sqlite::SqliteRow;
use sqlx::{Column, Sqlite};
use sqlx::{Row, SqlitePool};

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
    tablename: &str,
    headers: Vec<String>,
) -> Result<()> {
    let mut sql = Vec::<String>::new();
    let insert_starter = format!("INSERT INTO {} ({}) VALUES ", tablename, headers.join(","));

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

fn read_csv(
    filepath: &str,
    tablename: &str,
) -> Result<(String, Vec<String>, Reader<std::fs::File>)> {
    let input = std::fs::File::open(filepath)?;
    let mut reader = Reader::from_reader(input);
    let header: Vec<String> = reader.headers().unwrap().deserialize(None)?;
    if header.is_empty() {
        bail!("empty csv given in {}", filepath);
    }
    let mut sql = vec![
        format!("CREATE TABLE {}\n(", tablename),
        header.join(
            r#" NOT NULL DEFAULT '', 
        "#,
        ),
    ];
    sql.push(" NOT NULL DEFAULT '' ".to_string());
    sql.push(")".to_string());
    Ok((sql.join(" "), header, reader))
}

fn read_queried_data(run_q: &[SqliteRow], outfile: Option<String>) -> Result<()> {
    let mut results = Vec::<Vec<String>>::new();

    let row1 = run_q.iter().next();
    if row1.is_none() {
        bail!("no results found for the given query");
    }
    results.push(
        row1.unwrap()
            .columns()
            .iter()
            .map(|x| x.name().to_string())
            .collect(),
    );

    for row in run_q {
        let mut intermediary = Vec::<String>::new();
        for i in 0..row.len() {
            let v8: Vec<u8> = row.try_get_unchecked(i)?;
            let unescaped_str: String = std::str::from_utf8(&v8)?.to_string();
            intermediary.push(unescaped_str);
        }
        results.push(intermediary);
    }

    match outfile {
        None => {
            println!(
                "\nresults:\n\n{}",
                results
                    .iter()
                    .map(|x| x.join(","))
                    .collect::<Vec<String>>()
                    .iter()
                    .map(|x| format!("{}\n", x))
                    .collect::<String>()
            );
        }
        Some(outcsv) => {
            let mut writer = Writer::from_path(outcsv)?;
            for record in results {
                writer.write_record(record)?;
            }
        }
    };
    Ok(())
}

async fn create_tables(pool: &sqlx::Pool<Sqlite>, filepaths: Vec<String>) -> Result<()> {
    for (i, filepath) in filepaths.iter().enumerate() {
        let tablename = format!("table{}", i + 1);
        let (sql, headers, reader) = read_csv(filepath, &tablename)?;
        sqlx::query(&sql).execute(pool).await?;
        insert_data(pool, reader, &tablename, headers).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let pool = SqlitePool::connect("sqlite::memory:").await?;
    create_tables(&pool, cli.fileinput).await?;
    let run_q = sqlx::query(&cli.sql).fetch_all(&pool).await?;
    if run_q.is_empty() {
        bail!("no records returned for query:\n{}", cli.sql);
    }
    read_queried_data(&run_q, cli.outcsv)?;

    Ok(())
}
