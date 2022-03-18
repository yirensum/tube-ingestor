use tokio;
use neo4rs::{query, Graph, Query};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;

use serde::Deserialize;

mod tube_ingest;
mod bus_ingest;
use tube_ingest::run_tube_ingest;
use bus_ingest::run_bus_ingest;

#[tokio::main]
async fn main() {

    let uri = "127.0.0.1:7687";
    let user = "neo4j";
    let pass = "admin";
    let graph = Arc::new(Graph::new(&uri, user, pass).await.unwrap());

    //Transactions
    let mut txn = graph.start_txn().await.unwrap();
    txn.run_queries(vec![
        query("MATCH (n) DETACH DELETE n"),
    ])
        .await
        .unwrap();


    run_tube_ingest(&graph, &txn).await;
    run_bus_ingest(&graph, &txn).await;

    txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();

}
