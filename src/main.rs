use tokio;
use neo4rs::{query, Graph, Node, Query};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;
use std::io;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct Line {
    line: i32,
    name: String,
    colour: String,
}

#[derive(Debug, Deserialize, Clone)]
struct Station {
    id: i32,
    latitude: f32,
    longitude: f32,
    name: String,
    zone: f32,
    total_lines:  i32,
}

#[derive(Debug, Deserialize, Clone)]
struct CsvConnection {
    station1: i32,
    station2: i32,
    line: i32,
    time: i32,
}


#[derive(Debug, Deserialize)]
struct Connection {
    station1: Station,
    station2: Station,
    line: Line,
    time: i32,
}

fn parse_stations() -> Result<HashMap<i32, Station>, Box<dyn Error>> {
    let mut id_stations_map: HashMap<i32, Station> = HashMap::new();
    let mut rdr = csv::Reader::from_path("./datasets/london.stations.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let station: Station = result?;
        // println!("{:?}", station.clone());
        id_stations_map.insert(station.id, station);
    }
    Ok(id_stations_map)
}

fn parse_lines() -> Result<HashMap<i32, Line>, Box<dyn Error>> {
    let mut id_line_map: HashMap<i32, Line> = HashMap::new();
    let mut rdr = csv::Reader::from_path("./datasets/london.lines.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let line: Line = result?;
        // println!("{:?}", line.clone());
        id_line_map.insert(line.line, line);
    }
    Ok(id_line_map)
}

fn parse_connections(id_stations_map: &HashMap<i32, Station> , id_line_map: &HashMap<i32, Line>)
    -> Result<Vec<Connection>, Box<dyn Error>> {
    let mut connections: Vec<Connection> = Vec::new();
    let mut rdr = csv::Reader::from_path("./datasets/london.connections.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let csv_connection: CsvConnection = result?;
        let connection = Connection {
            station1: id_stations_map.get(&csv_connection.station1).cloned().unwrap(),
            station2: id_stations_map.get(&csv_connection.station2).cloned().unwrap(),
            line: id_line_map.get(&csv_connection.line).cloned().unwrap(),
            time: csv_connection.time,
        };
        connections.push(connection);
    }

    Ok(connections)
}

fn generate_node_creation_queries(id_stations_map: &HashMap<i32, Station>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for (id, station) in id_stations_map.into_iter() {
        queries.push(query("CREATE (s:Station {id: $id, x: $latitude, y: $longitude, \
        name: $name, zone: $zone, total_lines: $total_lines })")
            .param("id", station.id.clone().to_string())
            .param("latitude", station.latitude.clone().to_string())
            .param("longitude", station.longitude.clone().to_string())
            .param("name", station.name.clone())
            .param("zone", station.zone.clone().to_string())
            .param("total_lines", station.total_lines.clone().to_string())
        );
    }
    queries
}

#[tokio::main]
async fn main() {

    let id_stations_map = parse_stations().unwrap();
    let id_line_map = parse_lines().unwrap();
    let connections = parse_connections(&id_stations_map, &id_line_map).unwrap();

    let uri = "127.0.0.1:7687";
    let user = "neo4j";
    let pass = "admin";
    let graph = Arc::new(Graph::new(&uri, user, pass).await.unwrap());

    //Transactions
    let mut txn = graph.start_txn().await.unwrap();

    let node_creation_queries = generate_node_creation_queries(&id_stations_map);
    txn.run_queries(vec![
        query("MATCH (n) DETACH DELETE n"),
    ])
        .await
        .unwrap();

    txn.run_queries(node_creation_queries)
        .await
        .unwrap();
    txn.commit().await.unwrap(); //or txn.rollback().await.unwrap();
}
