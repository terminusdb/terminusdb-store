use std::env;

use std::io;
use terminus_store::storage::directory::NoFilenameEncoding;
use terminus_store::structure::TdbDataType;
use terminus_store::*;
use tokio;

async fn print_graph(store_path: &str, graph: &str) -> io::Result<()> {
    let store = open_directory_store(store_path, NoFilenameEncoding{});
    let graph = store
        .open(graph)
        .await?
        .expect(&format!("expected graph {} to exist", graph));

    match graph.head().await? {
        Some(layer) => {
            for id_triple in layer.triples() {
                // triples are retrieved in their id form. For printing,
                // we need the string form. The conversion happens here.
                let triple = layer
                    .id_triple_to_string(&id_triple)
                    .expect("expected id triple to be mapable to string");

                println!(
                    "{}, {}, {} {:?}",
                    triple.subject,
                    triple.predicate,
                    match triple.object {
                        ObjectType::Node(_) => "node",
                        ObjectType::Value(_) => "value",
                    },
                    match triple.object {
                        ObjectType::Node(n) => String::make_entry(&n),
                        ObjectType::Value(v) => v,
                    }
                );
            }
        }
        None => {}
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("usage: {} <path> <graph_name>", args[0]);
    } else {
        print_graph(&args[1], &args[2]).await.unwrap();
    }
}
