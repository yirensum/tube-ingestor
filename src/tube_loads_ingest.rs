use tokio;
use neo4rs::{query, Graph, Query, Txn};
use std::collections::{HashMap};
use std::sync::Arc;
use csv;
use std::error::Error;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct CsvLoadRow {
    Line: String,
    From_Station: String,
    To_Station: String,
    Total: String,
}

fn parse_tube_loads(file_path: String) -> Result<Vec<CsvLoadRow>, Box<dyn Error>> {
    let mut csv_load_vec: Vec<CsvLoadRow> = vec![];
    let mut rdr = csv::Reader::from_path(file_path).unwrap();
    for result in rdr.deserialize() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let csv_load_row: CsvLoadRow = result.unwrap();
        csv_load_vec.push(csv_load_row);
        // id_stations_map.insert(station.id, station);
    }
    Ok(csv_load_vec)
}

fn generate_tube_load_queries(tube_load_vec: Vec<CsvLoadRow>, load_string: String) -> Vec<Query> {
    let mut queries: Vec<Query> = Vec::new();
    for csv_load_row in tube_load_vec.into_iter() {

        let mut cleaned_line_name = csv_load_row.Line.clone();
        cleaned_line_name = cleaned_line_name.replace("&", "and");
        cleaned_line_name = cleaned_line_name.trim_end_matches(" ").parse().unwrap();
        cleaned_line_name = cleaned_line_name.replace(" ", "_");

        let mut _a = "MATCH (a:Station)-[r:".to_string();
        let mut _b = cleaned_line_name.to_uppercase();
        let mut _c = "]->(b:Station) WHERE a.name = $aname AND b.name = $bname SET r.".to_string();
        let mut _d = load_string.clone();
        let mut _e = " = $load".to_string();

        _a.push_str(&_b);
        _a.push_str(&_c);
        _a.push_str(&_d);
        _a.push_str(&_e);

        queries.push(query(&_a)
            .param("aname", csv_load_row.From_Station.clone().trim())
            .param("bname", csv_load_row.To_Station.clone().trim())
            .param("load", csv_load_row.Total.replace(',', "").parse::<i64>().unwrap()));
    }
    queries
}

pub async fn run_tube_load_ingest(graph: &Arc<Graph>, txn: &Txn) {

    let tube_load_vec_mtt = parse_tube_loads("./datasets/tube_link_loads_mtt.csv".to_string()).unwrap();
    let tube_load_vec_sat = parse_tube_loads("./datasets/tube_link_loads_sat.csv".to_string()).unwrap();

    let mut tube_load_queries = generate_tube_load_queries(tube_load_vec_mtt, "load_monday".to_string());
    txn.run_queries(tube_load_queries)
        .await
        .unwrap();

    let mut tube_load_queries = generate_tube_load_queries(tube_load_vec_sat, "load_saturday".to_string());
    txn.run_queries(tube_load_queries)
        .await
        .unwrap();

}
