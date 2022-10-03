use tokio;
use neo4rs::{query, Graph, Txn};
use std::sync::Arc;
mod tube;
mod bus;
mod tube_loads_ingest;
pub mod coordinate;
use tube::run_tube_ingest;

use crate::bus::{get_bus_stops, run_bus_ingest};
use crate::coordinate::{calculate_lat_bounds, calculate_long_bounds, Coordinate};
use crate::tube::get_tube_stations;

async fn clear_graph(txn: &Txn) {
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

    let txn = graph.start_txn().await.unwrap();
    clear_graph(&txn).await;
    txn.commit().await.unwrap();


    let mut tube_stations = get_tube_stations();
    let mut bus_stops = get_bus_stops();

    let (min_lat, max_lat) = calculate_lat_bounds(&tube_stations, None, None);
    let (min_lat, max_lat) = calculate_lat_bounds(&bus_stops, min_lat, max_lat);

    let (min_long, max_long) = calculate_long_bounds(&tube_stations, None, None);
    let (min_long, max_long) = calculate_long_bounds(&bus_stops, min_long, max_long);

    for tube_station in tube_stations.iter_mut() {
        tube_station.normalize_coordinates(min_lat.unwrap(), max_lat.unwrap(),
                                           min_long.unwrap(), max_long.unwrap());
    }
    for bus_stop in bus_stops.iter_mut() {
        bus_stop.normalize_coordinates(min_lat.unwrap(), max_lat.unwrap(),
                                       min_long.unwrap(), max_long.unwrap());
    }

    let txn = graph.start_txn().await.unwrap();
    run_tube_ingest(&graph, &txn, tube_stations).await;
    txn.commit().await.unwrap();

    // run_bus_ingest(&graph, bus_stops).await;

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
