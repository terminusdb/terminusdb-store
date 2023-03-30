use std::env;

use lazy_static::lazy_static;
use regex::Regex;
use terminus_store::storage::directory::NoFilenameEncoding;
use terminus_store::*;
use tokio;
use tokio::io::{self, AsyncBufReadExt};

enum Command {
    Add(ValueTriple),
    Remove(ValueTriple),
}

async fn parse_command(s: &str) -> io::Result<Command> {
    lazy_static! {
        static ref RE: Regex =
            Regex::new(r"(\S*)\s*(\S*)\s*,\s*(\S*)\s*,\s*(\S*)\s*(\S*)\s*").unwrap();
    }

    if let Some(matches) = RE.captures(s) {
        let command_name = &matches[1];
        let subject = &matches[2];
        let predicate = &matches[3];
        let object_type_name = &matches[4];
        let object = &matches[5];

        let triple = match object_type_name {
            "node" => ValueTriple::new_node(subject, predicate, object),
            "value" => ValueTriple::new_string_value(subject, predicate, object),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid object type {}", object_type_name),
                ))
            }
        };

        match command_name {
            "add" => Ok(Command::Add(triple)),
            "remove" => Ok(Command::Remove(triple)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid command {}", command_name),
            )),
        }
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("could not match line {}", s),
        ))
    }
}

async fn process_commands(store_path: &str, graph: &str) -> io::Result<()> {
    let store = open_directory_store(store_path, NoFilenameEncoding {});
    let graph = store
        .open(graph)
        .await?
        .expect(&format!("expected graph {} to exist", graph));

    // There are two types of builders. One creates a new base layer,
    // which has no parent. The other creates a child layer, which has
    // another layer as its parent.
    let builder = match graph.head().await? {
        Some(layer) => layer.open_write().await?,
        None => store.create_base_layer().await?,
    };
    let mut stdin = io::BufReader::new(io::stdin()).lines();

    while let Some(line) = stdin.next_line().await? {
        let segment = line.trim();
        if segment.len() == 0 {
            continue;
        }

        let command = parse_command(segment).await?;

        // add all the input data into the builder.
        // The builder keeps an in-memory list of added and removed
        // triples. If the same triple is added and removed on the
        // same builder, it is a no-op. This is even the case when it
        // is then later re-added on the same builder.
        //
        // Since no io is happening, adding triples to the builder is
        // not a future.
        match command {
            Command::Add(triple) => builder.add_value_triple(triple)?,
            Command::Remove(triple) => builder.remove_value_triple(triple)?,
        }
    }

    // When commit is called, the builder writes its data to
    // persistent storage.
    let layer = builder.commit().await?;

    // While a layer exists now, it's not yet attached to anything,
    // and is therefore unusable unless you know the exact identifier
    // of the layer itself. To make this the graph data, we have to
    // set the grap head to this layer.
    graph.set_head(&layer).await?;

    println!(
        "Added: {}, removed: {}",
        layer.triple_layer_addition_count().await?,
        layer.triple_layer_removal_count().await?
    );

    Ok(())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!(
            "usage: {} <path> <graph_name>
Commands should come from standard input, and should be of the following format:
  add subject, predicate, node object
  add subject, predicate, value object
  remove subject, predicate, node object
  remove subject, predicate, value object",
            args[0]
        );
    } else {
        process_commands(&args[1], &args[2]).await.unwrap();
    }
}
