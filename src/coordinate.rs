// traits - has a, inheritance - is a
pub trait Coordinate {
    fn get_lat(&self) -> f32;
    fn get_long(&self) -> f32;
    fn set_pos(&mut self, x: f32, y: f32);
    fn normalize_coordinates(&mut self, min_lat: f32, max_lat: f32, min_long: f32, max_long: f32) {
        let lat_range = max_lat - min_lat;
        let long_range = max_long - min_long;

        let min_x: f32 = -16000.0;
        let max_x: f32 = 16000.0;
        let min_y: f32 = -16000.0;
        let max_y: f32 = 16000.0;
        let width = max_x - min_x;
        let height = max_y - min_y;

        let x = (self.get_long() - min_long) / long_range * width + min_x;
        let y = -((self.get_lat() - min_lat) / lat_range * height + min_y); //HACK - bloom y-dir reversed?
        self.set_pos(x, y);
    }
}

pub fn calculate_lat_bounds<T: Coordinate>(stations: &Vec<T>, min_lat: Option<f32>, max_lat: Option<f32>)
                                           -> (Option<f32>, Option<f32>) {

    let latitudes: Vec<f32> = stations.iter()
        .map(|station| station.get_lat()).collect();

    let mut new_min_lat = *latitudes.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let mut new_max_lat = *latitudes.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();

    if min_lat != None && max_lat != None {
        new_min_lat = if new_min_lat <= min_lat.unwrap() { new_min_lat } else { min_lat.unwrap() };
        new_max_lat = if new_max_lat >= max_lat.unwrap() { new_max_lat } else { max_lat.unwrap() };
    }

    (Some(new_min_lat), Some(new_max_lat))
}

pub fn calculate_long_bounds<T: Coordinate>(stations: &Vec<T>, min_long: Option<f32>, max_long: Option<f32>)
                                            -> (Option<f32>, Option<f32>) {

    let longitudes: Vec<f32> = stations.iter()
        .map(|station| station.get_long()).collect();

    let mut new_min_long = *longitudes.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();
    let mut new_max_long = *longitudes.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap();

    if min_long != None && min_long != None {
       new_min_long = if new_min_long <= min_long.unwrap() { new_min_long } else { min_long.unwrap() };
       new_max_long = if new_max_long >= max_long.unwrap() { new_max_long } else { max_long.unwrap() };
    }

    (Some(new_min_long), Some(new_max_long))
}


// let lat_range = max_lat - min_lat;
// let long_range = max_long - min_long;
//
// let min_x: f32 = -16000.0;
// let max_x: f32 = 16000.0;
// let min_y: f32 = -16000.0;
// let max_y: f32 = 16000.0;
// let width = max_x - min_x;
// let height = max_y - min_y;
//
// let mut new_ids_stations_map: HashMap<String, Station> = HashMap::new();
// for (station, csv_station) in csv_stations_map.into_iter() {
// let new_station = Station {
// name: csv_station.Station.clone(),
// zone: csv_station.Zone.clone(),
// latitude: csv_station.Latitude,
// longitude: csv_station.Longitude,
// x: (csv_station.Longitude - min_long) / long_range * width + min_x,
// y: -((csv_station.Latitude - min_lat) / lat_range * height + min_y), //HACK - bloom y-dir reversed?
// postcode: csv_station.Postcode.clone()
// };
// new_ids_stations_map.insert(csv_station.Station.clone(), new_station);
// }
//
// new_ids_stations_map
//
//
// pub fn run_ingest() {
//
//
// }