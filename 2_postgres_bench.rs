#![feature(test)]

extern crate postgres;
extern crate test;

use self::postgres::*;
use self::postgres::types::ToSql;
use self::test::Bencher;
use std::env;

struct User {
    id: i32,
    name: String,
    hair_color: Option<String>,
}

struct Post {
    id: i32,
    user_id: i32,
    title: String,
    body: Option<String>,
}

fn connection() -> Connection {
    let database_url = env::var("DATABASE_URL").unwrap();
    let conn = Connection::connect(&*database_url, TlsMode::None).unwrap();
    conn.execute("TRUNCATE TABLE users, posts RESTART IDENTITY", &[]).unwrap();
    conn
}

fn benchmark_simple_query(num_rows: usize, b: &mut Bencher) {
    let conn = connection();

    let mut query = "INSERT INTO users (name) VALUES ".to_string();
    let mut binds = Vec::new();
    for i in 0..num_rows {
        if i != 0 {
            query += ", ";
        }
        query += &format!("(${})", i + 1);
        binds.push(format!("User {}", i));
    }
    if num_rows != 0 {
        let binds_borrowed_because_reasons = binds.iter().map(|b| &*b as &ToSql).collect::<Vec<_>>();
        assert_eq!(num_rows as u64, conn.execute(&query, &binds_borrowed_because_reasons).unwrap());
    }

    b.iter(|| {
        let users = conn.query("SELECT * FROM users", &[]).unwrap().into_iter()
            .map(|row| User {
                id: row.get("id"),
                name: row.get("name"),
                hair_color: row.get("hair_color"),
            }).collect::<Vec<_>>();
        assert_eq!(num_rows, users.len());
    })
}

fn benchmark_complex_query(num_rows: usize, b: &mut Bencher) {
    let conn = connection();
    let mut query = "INSERT INTO users (name, hair_color) VALUES ".to_string();
    let mut posts_query = "INSERT INTO posts (user_id, title, body) VALUES ".to_string();
    let mut binds = vec![];
    let mut post_binds = Vec::<Box<ToSql>>::new();
    for i in (0..num_rows) {
        if i != 0 {
            query += ", ";
        }
        query += &format!("(${}, ${})", binds.len()+1, binds.len()+2);
        let hair_color = if i % 2 == 0 { "black" } else { "brown" };
        binds.push(format!("User {}", i));
        binds.push(hair_color.to_string());

        if i % 3 == 0 {
            let len = post_binds.len();
            if len != 0 {
                posts_query += ", ";
            }
            posts_query += &format!("(${}, ${}, ${})", len + 1, len + 2, len + 3);
            post_binds.push(Box::new(i as i32 + 1));
            post_binds.push(Box::new("My first post"));
            post_binds.push(Box::new("This is the body of my first post"));
        }
    }
    if num_rows != 0 {
        let binds_borrowed_because_reasons_i_guess = binds.iter().map(|s| &*s as &postgres::types::ToSql).collect::<Vec<_>>();
        assert_eq!(num_rows as u64, conn.execute(&query, &*binds_borrowed_because_reasons_i_guess).unwrap());
    }

    if post_binds.len() != 0 {
        let binds_borrowed_because_reasons_i_guess = post_binds.iter().map(|s| &**s).collect::<Vec<_>>();
        conn.execute(&posts_query, &*binds_borrowed_because_reasons_i_guess).unwrap();
    }

    b.iter(|| {
        let query = "SELECT
            users.id as user_id,
            users.name as user_name,
            users.hair_color as user_hair_color,
            posts.id as post_id,
            posts.user_id as post_user_id,
            posts.title as post_title,
            posts.body as post_body
        FROM users LEFT OUTER JOIN posts ON
            users.id = posts.user_id
        WHERE users.hair_color = $1
        ORDER BY name DESC";
        let data = conn.query(query, &[&"black"]).unwrap().into_iter()
            .map(|row| {
                let user = User {
                    id: row.get("user_id"),
                    name: row.get("user_name"),
                    hair_color: row.get("user_hair_color"),
                };

                let post_id: Option<i32> = row.get("post_id");
                let post = post_id.map(|id|
                    Post {
                        id: id,
                        user_id: row.get("post_user_id"),
                        title: row.get("post_title"),
                        body: row.get("post_body"),
                    }
                );
                (user, post)
            })
            .collect::<Vec<_>>();

        let expected_row_count = (num_rows as f64 / 2.0).ceil() as usize;
        assert_eq!(expected_row_count, data.len());
    })
}

#[bench]
fn bench_simple_query_10_000_rows_postgres(b: &mut Bencher) {
    benchmark_simple_query(10_000, b)
}

#[bench]
fn bench_simple_query__1_000_rows_postgres(b: &mut Bencher) {
    benchmark_simple_query(1_000, b)
}

#[bench]
fn bench_simple_query____100_rows_postgres(b: &mut Bencher) {
    benchmark_simple_query(100, b)
}

#[bench]
fn bench_simple_query_____10_rows_postgres(b: &mut Bencher) {
    benchmark_simple_query(10, b)
}

#[bench]
fn bench_simple_query______1_rows_postgres(b: &mut Bencher) {
    benchmark_simple_query(1, b)
}

#[bench]
fn bench_simple_query______0_rows_postgres(b: &mut Bencher) {
    benchmark_simple_query(0, b)
}

#[bench]
fn bench_complex_query__1_000_rows_postgres(b: &mut Bencher) {
    benchmark_complex_query(1_000, b)
}

#[bench]
fn bench_complex_query____100_rows_postgres(b: &mut Bencher) {
    benchmark_complex_query(100, b)
}

#[bench]
fn bench_complex_query_____10_rows_postgres(b: &mut Bencher) {
    benchmark_complex_query(10, b)
}

#[bench]
fn bench_complex_query______1_rows_postgres(b: &mut Bencher) {
    benchmark_complex_query(1, b)
}

#[bench]
fn bench_complex_query______0_rows_postgres(b: &mut Bencher) {
    benchmark_complex_query(0, b)
}