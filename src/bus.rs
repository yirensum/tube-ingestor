use neo4rs::{query, Graph, Query};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;
use serde::Deserialize;
use crate::coordinate::Coordinate;

#[derive(Debug, Deserialize, Clone)]
struct CsvBusStop {
    Bus_Stop_Code: i64,
    Stop_Name: String,
    Latitude: f32,
    Longitude: f32,
}

#[derive(Debug, Deserialize, Clone)]
struct CsvRoute {
    Route: String,
    Run: i64,
    Sequence: i64,
    Stop_Name: String,
    Bus_Stop_Code: i64,
}

pub struct BusStop {
    name: String,
    latitude: f32,
    longitude: f32,
    bus_stop_code: i64,
    x: f32,
    y: f32,
}

impl Coordinate for BusStop {
    fn get_lat(&self) -> f32 {
        self.latitude
    }

    fn get_long(&self) -> f32 {
        self.longitude
    }

    fn set_pos(&mut self, x: f32, y: f32) {
        self.x = x;
        self.y = y
    }
}

struct Route<'a> {
    route_id: String,
    stop1: &'a BusStop,
    stop2: &'a BusStop,
}

fn parse_stops() -> Result<Vec<CsvBusStop>, Box<dyn Error>> {
    let mut csv_stops = Vec::new();
    let mut rdr = csv::Reader::from_path("./datasets/stops.csv").unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let stop: Option<CsvBusStop> = match result {
            Ok(obj) => Some(obj),
            Err(_err) => None,
        };
        if stop.is_some() {
            let new_stop = stop.unwrap();
            csv_stops.push(new_stop);
        }
    }
    Ok(csv_stops)
}

fn parse_routes<'a>(id_stops_map: &'a HashMap<i64, &BusStop>)
                -> Result<Vec<Route<'a>>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path("./datasets/busRoutes.csv").unwrap();

    // find a way to do this better
    let mut csv_routes: Vec<CsvRoute> = Vec::new();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.

        let csv_route: Option<CsvRoute> = match result {
            Ok(obj) => Some(obj),
            Err(_err) => None,
        };
        if csv_route.is_some() {
            csv_routes.push(csv_route.unwrap());
        }
    };

    let mut routes: Vec<Route> = Vec::new();

    for (a, b) in csv_routes.iter().zip(csv_routes.iter().skip(1)) {
        if a.Route == b.Route && a.Run == b.Run {
            // println!("{:?}", a);
            if id_stops_map.contains_key(&a.Bus_Stop_Code) && id_stops_map.contains_key(&b.Bus_Stop_Code) {
                let route = Route {
                    stop1: id_stops_map.get(&a.Bus_Stop_Code).unwrap(),
                    stop2: id_stops_map.get(&b.Bus_Stop_Code).unwrap(),
                    route_id: b.Route.clone()
                };
                routes.push(route)
            }
        }
    }
    Ok(routes)
}

fn convert_to_stops(csv_bus_stops: Vec<CsvBusStop>) -> Vec<BusStop> {
    let mut bus_stops = Vec::new();
    for csv_bus_stop in csv_bus_stops.into_iter() {
        let new_stop = BusStop {
            name: csv_bus_stop.Stop_Name.clone(),
            bus_stop_code: csv_bus_stop.Bus_Stop_Code,
            latitude: csv_bus_stop.Latitude,
            longitude: csv_bus_stop.Longitude,
            x: 0.0,
            y: 0.0
        };
        bus_stops.push(new_stop);
    }

    bus_stops
}

fn generate_node_creation_queries(id_stops_map: &Vec<BusStop>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for stop in id_stops_map.iter() {

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

pub fn get_bus_stops() -> Vec<BusStop> {
    let csv_bus_stops = parse_stops().unwrap();
    let bus_stops = convert_to_stops(csv_bus_stops);
    bus_stops
}

pub async fn run_bus_ingest(graph: &Arc<Graph>, bus_stops: Vec<BusStop>) {

    let mut stops_map = HashMap::new();
    for stop in bus_stops.iter() {
        stops_map.insert(stop.bus_stop_code, stop);
    }

    let routes = parse_routes(&stops_map).unwrap();

    let mut queries = Vec::new();
    let node_creation_queries = generate_node_creation_queries(&bus_stops);
    let route_creation_queries = generate_route_queries(&routes);

    queries.extend(node_creation_queries);
    queries.extend(route_creation_queries);

    let mut txn = graph.start_txn().await.unwrap();

    let query_chunks: Vec<&[Query]> = queries.chunks(10000).collect();

    for chunk in query_chunks {
        println!("{:?}", chunk.len());
        txn.run_queries(chunk.to_vec()).await.unwrap();
        txn.commit().await.unwrap();
        txn = graph.start_txn().await.unwrap();
    }
}







