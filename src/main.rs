use std::{env, fmt, path::PathBuf, io::Error};
use sqlx::{MySqlPool, MySql, mysql::MySqlPoolOptions, QueryBuilder, Execute};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use chrono::{Utc, DateTime};
use std::sync::Arc;
use http::Uri;


#[derive(Debug, Clone)]
struct Card {
    card_id: u32,
    card_no: String,
    record: DateTime<Utc>

}

// #[derive(Debug)]
// struct Cafe {
//     id: i32,
//     name: String,
//     slug: String
// }


fn help() {
    println!("usage:
xml2db <xml_file>");
}

fn parse_args() -> Result<(PathBuf, Uri),Error> {
    let args: Vec<String> = env::args().collect();

    match args.len() {
        1 => {
            return Err(Error::new(std::io::ErrorKind::NotFound, "missing xml input"));
        },
        2 => {
            return Err(Error::new(std::io::ErrorKind::NotFound, "missing db uri output"));
        },
        3 => {
            let path_buf = PathBuf::from(args[1].as_str());
            let database_str = args[2].as_str().parse::<Uri>().unwrap();
            return Ok((path_buf, database_str));
        },
        // all the other cases
        _ => {
            return Err(Error::new(std::io::ErrorKind::InvalidFilename, "non-existing file input"));
        }
    }
}

impl fmt::Display for Card {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Card: no {}, id {}, record {}", self.card_no, self.card_id, self.record)
    }
}

// async fn get_cafes(pool: Arc<MySqlPool>) -> Result<Vec<Cafe>, sqlx::Error> {
//     let cafes = sqlx::query_as!(
//         Cafe,
//         r#"SELECT id, name, slug FROM cafes"#
//     )
//     .fetch_all(pool.as_ref())
//     .await?;

//     Ok(cafes)
// }


async fn insert_cards(pool: Arc<MySqlPool>, cards: Vec<Card>) {

    let tasks: Vec<_> = cards.chunks(100)
        .map(|chunk| { chunk.to_vec() })
        .map(|chunk| {
            let pool = pool.clone();
            return tokio::spawn(async move {
                let mut query_builder: QueryBuilder<MySql> = QueryBuilder::new(
                    r#"INSERT INTO cards_cache (card_fusion_id, card_number, record_create_ts) "#
                );
                query_builder.push_values(&chunk, |mut b, card| {
                    b.push_bind(card.card_id)
                        .push_bind(card.card_no.clone())
                        .push_bind(card.record);
                });
                let query = query_builder.build();
                let sql = query.sql();
                match query.execute(pool.as_ref()).await {
                    Ok(_) => (),
                    Err(err) => println!("{err} on {}", sql)
                }
                chunk
            });
        })
        .collect();

    let mut results = vec![];
    for task in tasks {
        results.push(task.await.unwrap())
    }

    println!("There were {} inserts done", results.len());
}


#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {

    match parse_args() {
        Ok((xml_path, db_uri)) => {

            let db = db_uri.to_string().clone();
            let database_url = db.as_str();
            
            let pool = Arc::new(MySqlPoolOptions::new().connect(database_url).await?);

//            let cafes = get_cafes(pool.clone()).await?;

            let mut reader = match Reader::from_file(xml_path.as_path()) {
                Ok(reader) => reader,
                Err(err) => panic!("XML could not be read! ({})", err.to_string())
            };
            reader.config_mut().trim_text(true);

            let mut buf = Vec::new();

            let mut cards: Vec<Card> = Vec::new();

            let mut card_id: u32 = 0;
            let mut card_no: String = String::from("");
            loop {
                match reader.read_event_into(&mut buf) {
                    Err(e) => panic!("Error at position {}: {:?}", reader.error_position(), e),
                    Ok(Event::Eof) => break,
                    Ok(Event::Start(e)) => {
                        match e.name().as_ref() {
                            b"id" => if let Ok(Event::Text(e)) = reader.read_event_into(&mut buf) {
                                card_id = e.unescape().unwrap().parse().expect(&format!("id {:?}, could not have been parsed", e.unescape().unwrap()));
                            },
                            b"number" => if let Ok(Event::Text(e)) = reader.read_event_into(&mut buf) {
                                card_no = e.unescape().unwrap().to_owned().to_string();
                            },
                            _ => (),
                        }
                    },
                    Ok(Event::End(e)) => {
                        match e.name().as_ref() {
                            b"row" => {
                                cards.push( Card { card_id,
                                                   card_no: card_no.clone(),
                                                   record: Utc::now()
                                });
                            },
                            _ => ()
                        }
                    },

                    // We're not interested in others at the time being
                    _ => (),
                }
            }
//            println!("The xml is {:?}; and has {:?} cards", path.as_path(), cards.len());

            insert_cards(pool, cards.clone()).await;

//            println!("Cafes: {:#?}", cafes);

        },
        Err(_) => help()
    }

    Ok(())
}
