use tokio;
use neo4rs::{query, Graph, Query, Txn};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;

use serde::Deserialize;

mod tube;
mod bus;
mod tube_loads_ingest;
pub mod station;

use tube::run_tube_ingest;
use tube_loads_ingest::run_tube_load_ingest;
use bus::{Bus_Ingest};
use crate::bus::get_bus_stops;
use crate::station::{calculate_lat_bounds, calculate_long_bounds, Station};
use crate::tube::get_tube_stations;

async fn clear_graph(txn: Txn) {
    // // uncomment this to delete graph
    txn.run_queries(vec![
        query("MATCH (n) DETACH DELETE n"),
    ])
    .await
    .unwrap();
}

#[tokio::main]
async fn main() {

    let uri = "127.0.0.1:7687";
    let user = "neo4j";
    let pass = "admin";
    let graph = Arc::new(Graph::new(&uri, user, pass).await.unwrap());

    let mut txn = graph.start_txn().await.unwrap();
    clear_graph(txn);

    let tube_stations = get_tube_stations();
    let bus_stops = get_bus_stops();

    let (min_lat, max_lat) = calculate_lat_bounds(&tube_stations, 0.0, 0.0);
    let (min_lat, max_lat) = calculate_lat_bounds(&bus_stops, min_lat, max_lat);

    let (min_long, max_long) = calculate_long_bounds(&tube_stations, 0.0, 0.0);
    let (min_long, max_long) = calculate_lat_bounds(&bus_stops, min_long, max_long);

    for mut tube_station in tube_stations.into_iter() {
        tube_station.normalize_coordinates(min_lat, max_lat, min_long, max_long);
    }
    for mut bus_stop in bus_stops.into_iter() {
        bus_stop.normalize_coordinates(min_lat, max_lat, min_long, max_long);
    }




    // let mut stations: Vec<Box<dyn Station>> = Vec::new();
    // stations.append(tube_stations.into_iter().map(|station| Box::new(station)).collect());
    // a.append(&mut b);
    // let ve
    // let lat_bounds = calculate_lat_bounds()



    // run_tube_ingest(&graph, &txn).await;
    // txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();
    // txn = graph.start_txn().await.unwrap();
    //
    // run_tube_load_ingest(&graph, &txn).await;
    // txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();
    //
    // // // uncomment this to run bus ingestion
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
