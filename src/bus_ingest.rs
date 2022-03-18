use tokio;
use neo4rs::{query, Graph, Query, Txn};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct CsvStop {
    Stop_Name: String,
    Latitude: f32,
    Longitude: f32,
}

#[derive(Debug, Deserialize, Clone)]
struct Stop {
    name: String,
    latitude: f32,
    longitude: f32,
    x: f32,
    y: f32,
}

#[derive(Debug, Deserialize, Clone)]
struct CsvRoute {
    Route: i64,
    Run: i64,
    Sequence: i64,
    Stop_Name: String,
}

#[derive(Debug, Deserialize)]
struct Route {
    stop1: Stop,
    stop2: Stop,
}

fn parse_stops() -> Result<HashMap<String, CsvStop>, Box<dyn Error>> {
    let mut id_stop_map: HashMap<String, CsvStop> = HashMap::new();
    let mut rdr = csv::Reader::from_path("./datasets/stops.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let stop: CsvStop = result?;
        // println!("{:?}", stop.clone());
        id_stop_map.insert(stop.Stop_Name.clone(), stop);
    }
    Ok(id_stop_map)
}

// fn parse_connections(id_stations_map: &HashMap<i64, CsvStation>, id_line_map: &HashMap<i64, CsvLine>)
//                      -> Result<Vec<Connection>, Box<dyn Error>> {
//     let mut connections: Vec<Connection> = Vec::new();
//     let mut rdr = csv::Reader::from_path("./datasets/london.connections.csv").unwrap();
//     for result in rdr.deserialize() {
//         // Notice that we need to provide a type hint for automatic
//         // deserialization.
//         let csv_connection: CsvConnection = result?;
//         let connection = Connection {
//             station1: id_stations_map.get(&csv_connection.station1).cloned().unwrap(),
//             station2: id_stations_map.get(&csv_connection.station2).cloned().unwrap(),
//             line: id_line_map.get(&csv_connection.line).cloned().unwrap(),
//             time: csv_connection.time,
//         };
//         connections.push(connection);
//     }
//
//     Ok(connections)
// }

fn normalize_stop_coordinates(id_csv_stops_map: &HashMap<String, CsvStop>) -> HashMap<String, Stop> {

    let latitudes: Vec<f32> = id_csv_stops_map
        .values()
        .map(|csv_stop| csv_stop.Latitude).collect();

    let longitudes: Vec<f32> = id_csv_stops_map
        .values()
        .map(|csv_stop| csv_stop.Longitude).collect();

    let min_lat = *latitudes.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let max_lat = *latitudes.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let min_long = *longitudes.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let max_long = *longitudes.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let lat_range = max_lat - min_lat;
    let long_range = max_long - min_long;

    let min_x: f32 = -8000.0;
    let max_x: f32 = 8000.0;
    let min_y: f32 = -8000.0;
    let max_y: f32 = 8000.0;
    let width = max_x - min_x;
    let height = max_y - min_y;

    let mut new_id_stops_map: HashMap<String, Stop> = HashMap::new();
    for (id, csv_stop) in id_csv_stops_map.into_iter() {
        let new_stop = Stop {
            name: csv_stop.Stop_Name.clone(),
            latitude: csv_stop.Latitude,
            longitude: csv_stop.Longitude,
            x: (csv_stop.Longitude - min_long) / long_range * width + min_x + 703.0 + 273.0,
            y: -((csv_stop.Latitude - min_lat) / lat_range * height + min_y) + 3621.0 +343.0,
            // The additions below are to fix the error between bus and tube
        };
        new_id_stops_map.insert(csv_stop.Stop_Name.clone(), new_stop);
    }

    new_id_stops_map
}

fn generate_node_creation_queries(id_stops_map: &HashMap<String, Stop>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for (id, stop) in id_stops_map.into_iter() {

        queries.push(query("CREATE (s:Stop {x: $x, y: $y, \
        name: $name})")
            .param("x", stop.x.clone().to_string())
            .param("y", stop.y.clone().to_string())
            .param("name", stop.name.clone())
        );
    }
    queries
}
//
// fn generate_connections_queries(csv_connections: &Vec<Connection>) -> Vec<Query> {
//     let mut queries: Vec<Query> = Vec::new();
//     for connection in csv_connections.into_iter() {
//
//         let mut _a = "MATCH (a:Station), (b:Station) WHERE a.name = $aname AND b.name = $bname
//         CREATE (a)-[r:".to_string();
//         let _b = connection.line.name.clone().to_uppercase();
//         let _c = "{time: $time}]->(b)".to_string();
//
//         _a.push_str(&_b);
//         _a.push_str(&_c);
//
//         queries.push(query(&_a)
//             .param("aname", connection.station1.name.clone())
//             .param("bname", connection.station2.name.clone())
//             .param("time", connection.time.clone()));
//
//         queries.push(query(&_a)
//             .param("aname", connection.station2.name.clone())
//             .param("bname", connection.station1.name.clone())
//             .param("time", connection.time.clone()));
//     }
//     queries
// }

pub async fn run_bus_ingest(graph: &Arc<Graph>, txn: &Txn) {

    let id_csv_stops_map = parse_stops().unwrap();
    let id_stops_map = normalize_stop_coordinates(&id_csv_stops_map);

    // let connections = parse_connections(&id_csv_stations_map, &id_line_map).unwrap();
    //
    let node_creation_queries = generate_node_creation_queries(&id_stops_map);
    // let connection_creation_queries = generate_connections_queries(&connections);
    // txn.run_queries(vec![
    //     query("MATCH (n) DETACH DELETE n"),
    // ])
    //     .await
    //     .unwrap();
    //
    txn.run_queries(node_creation_queries)
        .await
        .unwrap();
    // txn.run_queries(connection_creation_queries)
    //     .await
    //     .unwrap();
    // txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();
}
