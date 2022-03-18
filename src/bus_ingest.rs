use tokio;
use neo4rs::{query, Graph, Query, Txn};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct CsvStop {
    Bus_Stop_Code: i64,
    Stop_Name: String,
    Latitude: f32,
    Longitude: f32,
}

#[derive(Debug, Deserialize, Clone)]
struct Stop {
    name: String,
    latitude: f32,
    longitude: f32,
    bus_stop_code: i64,
    x: f32,
    y: f32,
}

#[derive(Debug, Deserialize, Clone)]
struct CsvRoute {
    Route: String,
    Run: i64,
    Sequence: i64,
    Stop_Name: String,
    Bus_Stop_Code: i64,
}

#[derive(Debug, Deserialize)]
struct Route {
    route_id: String,
    stop1: Stop,
    stop2: Stop,
}

fn parse_stops() -> Result<HashMap<i64, CsvStop>, Box<dyn Error>> {
    let mut id_stop_map: HashMap<i64, CsvStop> = HashMap::new();
    let mut rdr = csv::Reader::from_path("./datasets/stops.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let stop: Option<CsvStop> = match (result) {
            Ok(obj) => Some(obj),
            Err(err) => None,
        };
        if stop.is_some() {
            let new_stop = stop.unwrap();
            id_stop_map.insert(new_stop.Bus_Stop_Code, new_stop);
        }
    }
    Ok(id_stop_map)
}

fn parse_routes(id_stops_map: &HashMap<i64, Stop>)
                -> Result<Vec<Route>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path("./datasets/busRoutes.csv").unwrap();

    // find a way to do this better
    let mut csv_routes: Vec<CsvRoute> = Vec::new();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.

        let csv_route: Option<CsvRoute> = match (result) {
            Ok(obj) => Some(obj),
            Err(err) => None,
        };
        if csv_route.is_some() {
            csv_routes.push(csv_route.unwrap());
        }
    };

    let mut routes: Vec<Route> = Vec::new();

    for (a, b) in csv_routes.iter().zip(csv_routes.iter().skip(1)) {
        if (a.Route == b.Route && a.Run == b.Run) {
            // println!("{:?}", a);
            if (id_stops_map.contains_key(&a.Bus_Stop_Code) && id_stops_map.contains_key(&b.Bus_Stop_Code)) {
                let route = Route {
                    stop1: id_stops_map.get(&a.Bus_Stop_Code).cloned().unwrap(),
                    stop2: id_stops_map.get(&b.Bus_Stop_Code).cloned().unwrap(),
                    route_id: b.Route.clone()
                };
                routes.push(route)
            }
        }
    }
    Ok(routes)
}

fn normalize_stop_coordinates(id_csv_stops_map: &HashMap<i64, CsvStop>) -> HashMap<i64, Stop> {

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

    let mut new_id_stops_map: HashMap<i64, Stop> = HashMap::new();
    for (id, csv_stop) in id_csv_stops_map.into_iter() {
        let new_stop = Stop {
            name: csv_stop.Stop_Name.clone(),
            bus_stop_code: csv_stop.Bus_Stop_Code,
            latitude: csv_stop.Latitude,
            longitude: csv_stop.Longitude,
            x: (csv_stop.Longitude - min_long) / long_range * width + min_x + 703.0 + 273.0,
            y: -((csv_stop.Latitude - min_lat) / lat_range * height + min_y) + 3621.0 +343.0,
            // The additions below are to fix the error between bus and tube
        };
        new_id_stops_map.insert(csv_stop.Bus_Stop_Code, new_stop);
    }

    new_id_stops_map
}

fn generate_node_creation_queries(id_stops_map: &HashMap<i64, Stop>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for (id, stop) in id_stops_map.into_iter() {

        queries.push(query("CREATE (s:Stop {x: $x, y: $y, \
        name: $name, bus_stop_code: $bus_stop_code})")
            .param("x", stop.x.clone().to_string())
            .param("y", stop.y.clone().to_string())
            .param("name", stop.name.clone())
            .param("bus_stop_code", stop.bus_stop_code)
        );
    }
    queries
}

fn generate_route_queries(routes: &Vec<Route>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for route in routes.into_iter() {

        queries.push(query("MATCH (a:Stop), (b:Stop) WHERE a.bus_stop_code = $bus_stop_code1
        AND b.bus_stop_code = $bus_stop_code2 CREATE (a)-[r: _ROUTE_ {route_id: $route_id}]->(b)")
            .param("bus_stop_code1", route.stop1.bus_stop_code)
            .param("bus_stop_code2", route.stop2.bus_stop_code)
            .param("route_id", route.route_id.clone()));
    }
    queries
}

pub struct Bus_Ingest {
    pub queries: Vec<Query>,
}

impl Bus_Ingest {
    pub fn new() -> Self {
        Bus_Ingest {
            queries: Vec::new()
        }
    }

    pub async fn run_bus_ingest(&mut self) {

        let id_csv_stops_map = parse_stops().unwrap();
        let id_stops_map = normalize_stop_coordinates(&id_csv_stops_map);

        let routes = parse_routes(&id_stops_map).unwrap();

        let node_creation_queries = generate_node_creation_queries(&id_stops_map);
        let route_creation_queries = generate_route_queries(&routes);

        self.queries = node_creation_queries;
        self.queries.extend(route_creation_queries);

    }
}






