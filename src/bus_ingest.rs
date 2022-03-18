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
    Route: String,
    Run: i64,
    Sequence: i64,
    Stop_Name: String,
}

#[derive(Debug, Deserialize)]
struct Route {
    route_id: String,
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

fn parse_routes(id_stops_map: &HashMap<String, Stop>)
                -> Result<Vec<Route>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path("./datasets/busRoutes.csv").unwrap();

    // find a way to do this better
    let mut csv_routes: Vec<CsvRoute> = Vec::new();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let csv_route: CsvRoute = result?;
        csv_routes.push(csv_route);

    };

    let mut routes: Vec<Route> = Vec::new();

    for (a, b) in csv_routes.iter().zip(csv_routes.iter().skip(1)) {
        if (a.Route == b.Route && a.Run == b.Run) {
            // println!("{:?}", a);
            if (id_stops_map.contains_key(&a.Stop_Name) && id_stops_map.contains_key(&b.Stop_Name)) {
                let route = Route {
                    stop1: id_stops_map.get(&a.Stop_Name).cloned().unwrap(),
                    stop2: id_stops_map.get(&b.Stop_Name).cloned().unwrap(),
                    route_id: b.Route.clone()
                };
                routes.push(route)
            }
        }
    }
    Ok(routes)
}

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

fn generate_route_queries(routes: &Vec<Route>) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for route in routes.into_iter() {

        queries.push(query("MATCH (a:Stop), (b:Stop) WHERE a.name = $aname AND b.name = $bname
        CREATE (a)-[r: _ROUTE_ {route_id: $route_id}]->(b)")
            .param("aname", route.stop1.name.clone())
            .param("bname", route.stop2.name.clone())
            .param("route_id", route.route_id.clone()));
    }
    queries
}

pub async fn run_bus_ingest(graph: &Arc<Graph>, txn: &Txn) {

    let id_csv_stops_map = parse_stops().unwrap();
    let id_stops_map = normalize_stop_coordinates(&id_csv_stops_map);

    let routes = parse_routes(&id_stops_map).unwrap();

    let node_creation_queries = generate_node_creation_queries(&id_stops_map);
    let route_creation_queries = generate_route_queries(&routes);
    txn.run_queries(node_creation_queries)
        .await
        .unwrap();
    txn.run_queries(route_creation_queries)
        .await
        .unwrap();
}
