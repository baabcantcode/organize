use sqlx::sqlite::SqlitePool;
use std::env;
use dotenvy::dotenv;
use tokio::sync::OnceCell;
use anyhow::Result;

async fn make_pool_result() -> Result<sqlx::Pool<sqlx::Sqlite>> {
    dotenv().ok();
    let dbfile = &env::var("DATABASE_URL")?;
    let pool = SqlitePool::connect(dbfile).await?;
    Ok(pool)
}

static POOL: OnceCell<sqlx::Pool<sqlx::Sqlite>> = OnceCell::const_new();

/// yeah so this library can panic here :x
/// would only happen if the library turned out to be entirely useless, so theres that
async fn get_pool() -> &'static sqlx::Pool<sqlx::Sqlite> {
    POOL.get_or_init(|| async { make_pool_result().await.unwrap() }).await
}

pub async fn add_todo(description: String) -> Result<i64> {
    let mut conn = get_pool().await.acquire().await?;

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
    .execute(get_pool().await)
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
    .fetch_all(get_pool().await)
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
