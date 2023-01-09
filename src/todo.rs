use anyhow::Result;
use async_once::AsyncOnce;
use dotenvy::dotenv;
use lazy_static::lazy_static;
use sqlx::sqlite::SqlitePool;
use sqlx::{Pool, Sqlite};
use std::env;

lazy_static! {
/// will panic if DATABASE_URL is invalid or a connection cannot
/// be established
    static ref POOL: AsyncOnce<Pool<Sqlite>> = AsyncOnce::new(async {
        dotenv().ok();
        let dbfile = &env::var("DATABASE_URL").unwrap();
        SqlitePool::connect(dbfile).await.unwrap()
    });
}

pub async fn add_todo(description: String) -> Result<i64> {
    let mut conn = POOL.get().await.acquire().await?;

    // Insert the task, then obtain the ID of this row
    let id = sqlx::query!(
        r#"
INSERT INTO todos ( description )
VALUES ( ?1 )
        "#,
        description
    )
    .execute(&mut conn)
    .await?
    .last_insert_rowid();

    Ok(id)
}

pub async fn complete_todo(id: i64) -> Result<bool> {
    let rows_affected = sqlx::query!(
        r#"
UPDATE todos
SET done = TRUE
WHERE id = ?1
        "#,
        id
    )
    .execute(POOL.get().await)
    .await?
    .rows_affected();

    Ok(rows_affected > 0)
}

pub async fn list_todos() -> Result<()> {
    let recs = sqlx::query!(
        r#"
SELECT id, description, done
FROM todos
ORDER BY id
        "#
    )
    .fetch_all(POOL.get().await)
    .await?;

    for rec in recs {
        println!(
            "- [{}] {}: {}",
            if rec.done { "x" } else { " " },
            rec.id,
            &rec.description,
        );
    }

    Ok(())
}
