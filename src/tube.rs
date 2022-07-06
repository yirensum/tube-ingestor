use neo4rs::{query, Graph, Query, Txn};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;
use serde::{Deserialize, Serialize};
use crate::coordinate::Coordinate;

#[derive(Clone, Serialize, Deserialize)]
struct Line(String);

#[derive(Clone, Serialize, Deserialize)]
struct CsvTubeStation {
    Latitude: f32,
    Longitude: f32,
    Station: String,
    Zone: String,
    Postcode: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct CsvConnection {
    Tube_Line: String,
    From_Station: String,
    To_Station: String,
}

pub struct TubeStation {
    x: f32,
    y: f32,
    name: String,
    postcode: String,
    latitude: f32,
    longitude: f32,
    zone: String,
}

impl Coordinate for TubeStation {
    fn get_lat(&self) -> f32 {
        self.latitude
    }

    fn get_long(&self) -> f32 {
        self.longitude
    }

    fn set_pos(&mut self, x: f32, y: f32) {
        self.x = x;
        self.y = y;
    }
}

struct Connection<'a> {
    station1: &'a TubeStation,
    station2: &'a TubeStation,
    line: Line,
}

fn parse_stations() -> Result<Vec<CsvTubeStation>, Box<dyn Error>> {
    let mut csv_stations = Vec::new();
    let mut rdr = csv::Reader::from_path("./datasets/London_stations.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let station: CsvTubeStation = result?;
        // println!("{:?}", station.clone());
        csv_stations.push(station);
    }
    Ok(csv_stations)
}

fn parse_connections<'a>(stations_map: &'a HashMap<String, &TubeStation>)
                     -> Result<Vec<Connection<'a>>, Box<dyn Error>> {
    let mut connections: Vec<Connection> = Vec::new();
    let mut rdr = csv::Reader::from_path("./datasets/London_tube_lines.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let csv_connection: CsvConnection = result?;
        // println!("{} - {}", &csv_connection.From_Station,  &csv_connection.To_Station);

        let line_name = csv_connection.Tube_Line.clone();
        let mut cleaned_line_name = line_name.replace("Line", "");
        cleaned_line_name = cleaned_line_name.replace("&", "and");
        cleaned_line_name = cleaned_line_name.trim_end_matches(" ").parse().unwrap();
        cleaned_line_name = cleaned_line_name.replace(" ", "_");
        let connection = Connection {
            station1: stations_map.get(&csv_connection.From_Station).unwrap(),
            station2: stations_map.get(&csv_connection.To_Station).unwrap(),
            line: Line(cleaned_line_name),
        };
        connections.push(connection);
    }

    Ok(connections)
}

fn convert_to_stations(csv_stations: Vec<CsvTubeStation>) -> Vec<TubeStation> {
    let mut stations = Vec::new();
    for csv_station in csv_stations.into_iter() {
        let new_station = TubeStation {
            name: csv_station.Station.clone(),
            zone: csv_station.Zone.clone(),
            latitude: csv_station.Latitude,
            longitude: csv_station.Longitude,
            x: 0.0,
            y: 0.0,
            postcode: csv_station.Postcode.clone()
        };
        stations.push(new_station);
    }
    stations
}

fn generate_node_creation_queries(stations: &Vec<TubeStation>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for station in stations.iter() {

        queries.push(query("CREATE (s:Station {x: $x, y: $y, \
        name: $name, zone: $zone })")
            .param("x", station.x.clone().to_string())
            .param("y", station.y.clone().to_string())
            .param("name", station.name.clone())
            .param("zone", station.zone.clone().to_string())
        );
    }
    queries
}

fn generate_connections_queries(csv_connections: &Vec<Connection>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for connection in csv_connections.into_iter() {

        let mut _a = "MATCH (a:Station), (b:Station) WHERE a.name = $aname AND b.name = $bname
        CREATE (a)-[r:".to_string();
        let _b = connection.line.0.to_uppercase();
        let _c = "]->(b)".to_string();

        _a.push_str(&_b);
        _a.push_str(&_c);

        queries.push(query(&_a)
            .param("aname", connection.station1.name.clone())
            .param("bname", connection.station2.name.clone())
        );

        queries.push(query(&_a)
            .param("aname", connection.station2.name.clone())
            .param("bname", connection.station1.name.clone())
        );
    }
    queries
}

pub fn get_tube_stations() -> Vec<TubeStation> {
    let csv_stations = parse_stations().unwrap();
    let stations_map = convert_to_stations(csv_stations);
    stations_map
}

pub async fn run_tube_ingest(_graph: &Arc<Graph>, txn: &Txn, tube_stations: Vec<TubeStation>) {

    let mut stations_map = HashMap::new();
    for station in tube_stations.iter() {
        stations_map.insert(station.name.clone(), station);
    }
    let connections = parse_connections(&stations_map).unwrap();

    let node_creation_queries = generate_node_creation_queries(&tube_stations);
    let connection_creation_queries = generate_connections_queries(&connections);

    txn.run_queries(node_creation_queries)
        .await
        .unwrap();
    txn.run_queries(connection_creation_queries)
        .await
        .unwrap();

}
