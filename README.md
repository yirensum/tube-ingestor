# Tube Ingestor
### Running this ingests tube and bus station data into Neo4j.


## Data Sources
### Tube Data:
Taken from https://www.doogal.co.uk/london_stations.php and https://github.com/nicola/tubemaps/tree/master/datasets

Crowding data taken from: http://crowding.data.tfl.gov.uk/

London Tube colors: https://londonmymind.com/london-tube-colors/
### Bus Data:
Taken from: https://data.london.gov.uk/dataset/tfl-bus-stop-locations-and-routes

## Installation and Running

1) Install Cargo to run Rust (see here: https://www.rust-lang.org/tools/install)
2) Run `cargo install`
3) Change username, password in `main.rs` (todo - make this env vars)
4) Run `cargo run` (make sure desktop db is running)