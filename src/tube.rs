use tokio;
use neo4rs::{query, Graph, Query, Txn};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;
use serde::{Deserialize, Serialize};
use crate::station::Station;

// #[derive(Debug, Deserialize, Clone)]
// struct Line {
//     line: i64,
//     name: String,
//     colour: String,
// }

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

#[derive(Clone, Serialize, Deserialize)]
pub struct TubeStation {
    x: f32,
    y: f32,
    name: String,
    postcode: String,
    latitude: f32,
    longitude: f32,
    zone: String,
}

impl Station for TubeStation {
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

#[derive(Clone, Serialize, Deserialize)]
struct Connection {
    station1: TubeStation,
    station2: TubeStation,
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

// fn parse_lines() -> Result<HashMap<i64, Line>, Box<dyn Error>> {
//     let mut id_line_map: HashMap<i64, Line> = HashMap::new();
//     let mut rdr = csv::Reader::from_path("./datasets/london.lines.csv").unwrap();
//     for result in rdr.deserialize() {
//         // Notice that we need to provide a type hint for automatic
//         // deserialization.
//         let mut line: Line = result?;
//         let mut cleaned_line_name = line.name.clone();
//         cleaned_line_name = cleaned_line_name.replace("Line", "");
//         cleaned_line_name = cleaned_line_name.replace("&", "and");
//         cleaned_line_name = cleaned_line_name.trim_end_matches(" ").parse().unwrap();
//         cleaned_line_name = cleaned_line_name.replace(" ", "_");
//         line.name = cleaned_line_name;
//         id_line_map.insert(line.line, line);
//     }
//     Ok(id_line_map)
// }

fn parse_connections(stations_map: &HashMap<String, TubeStation>)
                     -> Result<Vec<Connection>, Box<dyn Error>> {
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
            station1: stations_map.get(&csv_connection.From_Station).cloned().unwrap(),
            station2: stations_map.get(&csv_connection.To_Station).cloned().unwrap(),
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

fn normalize_station_coordinates(csv_stations_map: &HashMap<String, CsvTubeStation>) -> HashMap<String, TubeStation> {

    let latitudes: Vec<f32> = csv_stations_map
        .values()
        .map(|csv_station| csv_station.Latitude).collect();

    let longitudes: Vec<f32> = csv_stations_map
        .values()
        .map(|csv_station| csv_station.Longitude).collect();

    let min_lat = *latitudes.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let max_lat = *latitudes.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let min_long = *longitudes.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let max_long = *longitudes.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let lat_range = max_lat - min_lat;
    let long_range = max_long - min_long;

    let min_x: f32 = -16000.0;
    let max_x: f32 = 16000.0;
    let min_y: f32 = -16000.0;
    let max_y: f32 = 16000.0;
    let width = max_x - min_x;
    let height = max_y - min_y;

    let mut new_ids_stations_map: HashMap<String, TubeStation> = HashMap::new();
    for (station, csv_station) in csv_stations_map.into_iter() {
        let new_station = TubeStation {
            name: csv_station.Station.clone(),
            zone: csv_station.Zone.clone(),
            latitude: csv_station.Latitude,
            longitude: csv_station.Longitude,
            x: (csv_station.Longitude - min_long) / long_range * width + min_x,
            y: -((csv_station.Latitude - min_lat) / lat_range * height + min_y), //HACK - bloom y-dir reversed?
            postcode: csv_station.Postcode.clone()
        };
        new_ids_stations_map.insert(csv_station.Station.clone(), new_station);
    }

    new_ids_stations_map
}

fn generate_node_creation_queries(id_stations_map: &HashMap<String, TubeStation>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for (name, station) in id_stations_map.into_iter() {

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

pub async fn run_tube_ingest(graph: &Arc<Graph>, txn: &Txn) {

    // let stations_map = normalize_station_coordinates(&id_csv_stations_map);
    // // let id_line_map = parse_lines().unwrap();
    // let connections = parse_connections(&stations_map).unwrap();
    //
    // let node_creation_queries = generate_node_creation_queries(&stations_map);
    // let connection_creation_queries = generate_connections_queries(&connections);
    //
    // txn.run_queries(node_creation_queries)
    //     .await
    //     .unwrap();
    // txn.run_queries(connection_creation_queries)
    //     .await
    //     .unwrap();
}
