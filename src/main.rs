use tokio;
use neo4rs::{query, Graph, Query};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;

use serde::Deserialize;

mod tube_ingest;
mod bus_ingest;
mod tube_loads_ingest;

use tube_ingest::run_tube_ingest;
use tube_loads_ingest::run_tube_load_ingest;
use bus_ingest::{Bus_Ingest};

#[tokio::main]
async fn main() {

    let uri = "127.0.0.1:7687";
    let user = "neo4j";
    let pass = "admin";
    let graph = Arc::new(Graph::new(&uri, user, pass).await.unwrap());

    //Transactions
    let mut txn = graph.start_txn().await.unwrap();

    // // uncomment this to delete graph
    txn.run_queries(vec![
        query("MATCH (n) DETACH DELETE n"),
    ])
        .await
        .unwrap();

    run_tube_ingest(&graph, &txn).await;
    txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();
    txn = graph.start_txn().await.unwrap();

    run_tube_load_ingest(&graph, &txn).await;
    txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();

    // // uncomment this to run bus ingestion
    // let mut bus_ingest = Bus_Ingest::new();
    // bus_ingest.run_bus_ingest().await;
    //
    // let mut txn = graph.start_txn().await.unwrap();
    //
    // let query_chunks: Vec<&[Query]> = bus_ingest.queries.chunks(10000).collect();
    //
    // for chunk in query_chunks {
    //     println!("{:?}", chunk.len());
    //     txn.run_queries(chunk.to_vec()).await.unwrap();
    //     txn.commit().await.unwrap();
    //     txn = graph.start_txn().await.unwrap();
    // }
}
