use tokio;
use neo4rs::{query, Graph, Query};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct CsvLine {
    line: i32,
    name: String,
    colour: String,
}

#[derive(Debug, Deserialize, Clone)]
struct CsvStation {
    id: i32,
    latitude: f32,
    longitude: f32,
    name: String,
    zone: f32,
    total_lines:  i32,
}

#[derive(Debug, Deserialize, Clone)]
struct Station {
    id: i32,
    x: f32,
    y: f32,
    name: String,
    zone: f32,
    total_lines:  i32,
    latitude: f32,
    longitude: f32,
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
    station1: CsvStation,
    station2: CsvStation,
    line: CsvLine,
    time: i32,
}

fn parse_stations() -> Result<HashMap<i32, CsvStation>, Box<dyn Error>> {
    let mut id_stations_map: HashMap<i32, CsvStation> = HashMap::new();
    let mut rdr = csv::Reader::from_path("./datasets/london.stations.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let station: CsvStation = result?;
        // println!("{:?}", station.clone());
        id_stations_map.insert(station.id, station);
    }
    Ok(id_stations_map)
}

fn parse_lines() -> Result<HashMap<i32, CsvLine>, Box<dyn Error>> {
    let mut id_line_map: HashMap<i32, CsvLine> = HashMap::new();
    let mut rdr = csv::Reader::from_path("./datasets/london.lines.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let line: CsvLine = result?;
        // println!("{:?}", line.clone());
        id_line_map.insert(line.line, line);
    }
    Ok(id_line_map)
}

fn parse_connections(id_stations_map: &HashMap<i32, CsvStation>, id_line_map: &HashMap<i32, CsvLine>)
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

fn normalize_station_coordinates(id_csv_stations_map: &HashMap<i32, CsvStation>) -> HashMap<i32, Station> {

    let latitudes: Vec<f32> = id_csv_stations_map
        .values()
        .map(|csv_station| csv_station.latitude).collect();

    let longitudes: Vec<f32> = id_csv_stations_map
        .values()
        .map(|csv_station| csv_station.longitude).collect();

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

    let mut new_ids_stations_map: HashMap<i32, Station> = HashMap::new();
    for (id, csv_station) in id_csv_stations_map.into_iter() {
        let new_station = Station {
            id: csv_station.id,
            name: csv_station.name.clone(),
            zone: csv_station.zone,
            total_lines: csv_station.total_lines,
            latitude: csv_station.latitude,
            longitude: csv_station.longitude,
            x: (csv_station.longitude - min_long) / long_range * width + min_x,
            y: -((csv_station.latitude - min_lat) / lat_range * height + min_y) //HACK - bloom y-dir reversed?
        };
        new_ids_stations_map.insert(csv_station.id, new_station);
    }

    new_ids_stations_map
}

fn generate_node_creation_queries(id_csv_stations_map: &HashMap<i32, CsvStation>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    let id_stations_map = normalize_station_coordinates(&id_csv_stations_map);
    for (id, station) in id_stations_map.into_iter() {
        queries.push(query("CREATE (s:Station {id: $id, x: $x, y: $y, \
        name: $name, zone: $zone, total_lines: $total_lines })")
            .param("id", station.id.clone().to_string())
            .param("x", station.x.clone().to_string())
            .param("y", station.y.clone().to_string())
            .param("name", station.name.clone())
            .param("zone", station.zone.clone().to_string())
            .param("total_lines", station.total_lines.clone().to_string())
        );
    }
    queries
}

#[tokio::main]
async fn main() {

    let id_csv_stations_map = parse_stations().unwrap();
    let id_line_map = parse_lines().unwrap();
    let connections = parse_connections(&id_csv_stations_map, &id_line_map).unwrap();

    let uri = "127.0.0.1:7687";
    let user = "neo4j";
    let pass = "admin";
    let graph = Arc::new(Graph::new(&uri, user, pass).await.unwrap());

    //Transactions
    let mut txn = graph.start_txn().await.unwrap();

    let node_creation_queries = generate_node_creation_queries(&id_csv_stations_map);
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
