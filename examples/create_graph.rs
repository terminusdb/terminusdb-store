use std::env;

use terminus_store::storage::directory::NoFilenameEncoding;
use terminus_store::*;
use tokio;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("usage: {} <path> <graph_name>", args[0]);
    } else {
        // open a store at the given path. the directory has to exist.
        let store = open_directory_store(&args[1], NoFilenameEncoding {});

        // then create a graph. if the graph already exists, this will error.
        store.create(&args[2]).await.unwrap();
    }
}
