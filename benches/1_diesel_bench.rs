#![feature(test, proc_macro)]

#[macro_use] extern crate diesel;
#[macro_use] extern crate diesel_codegen;
extern crate test;

infer_schema!("env:DATABASE_URL");

use self::diesel::*;
use self::diesel::pg::PgConnection;
use self::test::Bencher;
use std::env;

#[derive(Queryable, Identifiable, Associations)]
#[has_many(posts)]
pub struct User {
    id: i32,
    name: String,
    hair_color: Option<String>,
}

#[derive(Insertable)]
#[table_name="users"]
struct NewUser {
    name: String,
    hair_color: Option<String>,
}

#[derive(Queryable, Identifiable, Associations)]
#[belongs_to(User)]
struct Post {
    id: i32,
    user_id: i32,
    title: String,
    body: Option<String>,
}

#[derive(Insertable)]
#[table_name="posts"]
struct NewPost<'a> {
    user_id: i32,
    title: &'a str,
    body: Option<&'a str>,
}

fn connection() -> PgConnection {
    let database_url = env::var("DATABASE_URL").unwrap();
    let conn = PgConnection::establish(&database_url).unwrap();
    conn.execute("TRUNCATE TABLE users, posts RESTART IDENTITY").unwrap();
    conn
}

fn benchmark_simple_query(num_rows: usize, b: &mut Bencher) {
    let conn = connection();

    let data: Vec<_> = (0..num_rows).map(|i| {
        NewUser { name: format!("User {}", i), hair_color: None }
    }).collect();
    assert_eq!(Ok(num_rows), insert(&data).into(users::table).execute(&conn));

    b.iter(|| {
        assert_eq!(num_rows, users::table.load::<User>(&conn).unwrap().len());
    })
}

fn benchmark_complex_query(num_rows: usize, b: &mut Bencher) {
    let conn = connection();

    let mut posts = Vec::new();
    let data: Vec<_> = (0..num_rows).map(|i| {
        let hair_color = if i % 2 == 0 { "black" } else { "brown" };
        let user = NewUser { name: format!("User {}", i), hair_color: Some(hair_color.into()) };

        if i % 3 == 0 {
            posts.push(NewPost {
                user_id: i as i32 + 1,
                title: "My first post",
                body: Some("This is the body of my first post"),
            })
        }
        user
    }).collect();
    assert_eq!(Ok(num_rows), insert(&data).into(users::table).execute(&conn));
    assert_eq!(Ok(posts.len()), insert(&posts).into(posts::table).execute(&conn));

    b.iter(|| {
        use users::dsl::*;

        let query = users.left_outer_join(posts::table)
            .filter(hair_color.eq("black"))
            .order(name.desc());
        let expected_row_count = (num_rows as f64 / 2.0).ceil() as usize;
        assert_eq!(expected_row_count, query.load::<(User, Option<Post>)>(&conn).unwrap().len());
    })
}

#[bench]
fn bench_simple_query_10_000_rows_diesel(b: &mut Bencher) {
    benchmark_simple_query(10_000, b)
}

#[bench]
fn bench_simple_query__1_000_rows_diesel(b: &mut Bencher) {
    benchmark_simple_query(1_000, b)
}

#[bench]
fn bench_simple_query____100_rows_diesel(b: &mut Bencher) {
    benchmark_simple_query(100, b)
}

#[bench]
fn bench_simple_query_____10_rows_diesel(b: &mut Bencher) {
    benchmark_simple_query(10, b)
}

#[bench]
fn bench_simple_query______1_rows_diesel(b: &mut Bencher) {
    benchmark_simple_query(1, b)
}

#[bench]
fn bench_simple_query______0_rows_diesel(b: &mut Bencher) {
    benchmark_simple_query(0, b)
}

#[bench]
fn bench_complex_query__1_000_rows_diesel(b: &mut Bencher) {
    benchmark_complex_query(1_000, b)
}

#[bench]
fn bench_complex_query____100_rows_diesel(b: &mut Bencher) {
    benchmark_complex_query(100, b)
}

#[bench]
fn bench_complex_query_____10_rows_diesel(b: &mut Bencher) {
    benchmark_complex_query(10, b)
}

#[bench]
fn bench_complex_query______1_rows_diesel(b: &mut Bencher) {
    benchmark_complex_query(1, b)
}

#[bench]
fn bench_complex_query______0_rows_diesel(b: &mut Bencher) {
    benchmark_complex_query(0, b)
}