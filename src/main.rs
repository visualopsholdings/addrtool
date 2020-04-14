//! This is my first very rust program.
//!
//! Be kind.
//!
//!

use mongodb::{ Database, Client, options::ClientOptions, Collection };
use mongodb::options::{ FindOptions, FindOneOptions, DeleteOptions };
use bson::{ doc, Bson, Document };
use std::io::{ self, Write };
use std::str::FromStr;
use structopt::StructOpt;
use std::string::String;

/// Import tool for the either G-NAF australian address database or US open gov address data.
#[derive(StructOpt)]
struct Cli {

    #[structopt(help="The name of the DB in MongoDB")]
    db: String,
    
    #[structopt(long, help="G-NAF data", default_value="")]
    gnaf: String,

    #[structopt(long, help="US Open Government data")]
    opengov: bool,

    #[structopt(help="The name of the collection to create")]
    coll: String,
    
    #[structopt(help="An identifier for the STATE field", default_value="1")]
    state: i32,
    
    #[structopt(long, default_value="", help="The ID of a single record to process")]
    single: String,
	
    #[structopt(long, help="Drop the new collections before proceeding")]
    drop: bool,
	
    #[structopt(long, help="Skip creating the main collection")]
    nomain: bool,
	
    #[structopt(long, help="Skip creating the tuples collection")]
    notuples: bool
	
}

fn main() {

    let args = Cli::from_args();
    
	let client_options = ClientOptions::parse("mongodb://localhost:27017").unwrap();
	let client = Client::with_options(client_options).unwrap();
	// set your DB name here.
	let db = client.database(&args.db);
	
	// whatever you want the new DB to be called.
	let newcollection = db.collection(&args.coll);
	match newcollection.estimated_document_count(None) {
		Ok(count) => { 
			println!("{} in {} has {} documents.", args.coll, args.db, count); 
		},
		Err(e) => {
			panic!("can't get document count of {} in {}: {}", args.coll, args.db, e);
		}
 	}
	let newtuple = db.collection(format!("{}_tuples", args.coll).as_str());
	
	if args.drop {
		println!("dropping...");
		let _ = newcollection.drop(None);
		let _ = newtuple.drop(None);
	}

	if args.gnaf.len() > 0 {
		if args.single.len() > 0 {
			build_one_gnaf(&db, args.single.as_str(), args.state, args.gnaf.as_str(), &newcollection, &newtuple);
		}
		else {
			if args.nomain {
				if args.notuples {
					println!("Test run, nothing will happen");
					build_state_gnaf(&db, args.state, args.gnaf.as_str(), None, None);
				}
				else {
					println!("Only tuples.");
					build_state_gnaf(&db, args.state, args.gnaf.as_str(), None, Some(&newtuple));
				}
			}
			if args.notuples {
				println!("Only main collection.");
				build_state_gnaf(&db, args.state, args.gnaf.as_str(), Some(&newcollection), None);
			}
			else {
				build_state_gnaf(&db, args.state, args.gnaf.as_str(), Some(&newcollection), Some(&newtuple));
			}
		}
	}
	else if args.opengov {
		if args.single.len() > 0 {
			build_one_og(&db, args.single.as_str(), args.state, &newcollection, &newtuple);
		}
		else {
			if args.nomain {
				if args.notuples {
					println!("Test run, nothing will happen");
					build_state_og(&db, args.state, None, None);
				}
				else {
					println!("Only tuples.");
					build_state_og(&db, args.state, None, Some(&newtuple));
				}
			}
			if args.notuples {
				println!("Only main collection.");
				build_state_og(&db, args.state, Some(&newcollection), None);
			}
			else {
				build_state_og(&db, args.state, Some(&newcollection), Some(&newtuple));
			}
		}
	}
	else {
		println!("Need to set --opengov or --gnaf prefix.")
	}
}

fn build_state_gnaf(db: &Database, state: i32, name: &str, newcollection: Option<&Collection>, newtuple: Option<&Collection>) {
	println!("building {} state {}", name, state);
	_build_state_gnaf(state, 
		db.collection(format!("{}_address_detail", name).as_str()), 
		db.collection(format!("{}_street_locality", name).as_str()), 
		db.collection(format!("{}_locality", name).as_str()), 
		db.collection(format!("{}_address_default_geocode", name).as_str()), 
		newcollection, newtuple
	);
}

fn _build_state_gnaf(state: i32, detail: Collection, street: Collection, locality: Collection, geo: Collection, 
		newcollection: Option<&Collection>, newtuplescollection: Option<&Collection>) {
	
	match newcollection {
		Some(c) => {
			match c.delete_many(doc! { "STATE": state }, DeleteOptions::builder().build()) {
				Ok(result) => { println!("deleted {} in new collection", result.deleted_count) },
				Err(e) => { println!("err: {}", e); }
			}
		},
		None => ()
	}
	match newtuplescollection {
		Some(c) => {
			match c.delete_many(doc! { "STATE": state }, DeleteOptions::builder().build()) {
				Ok(result) => { println!("deleted {} in new tuples collection", result.deleted_count) },
				Err(e) => { println!("err: {}", e); }
			}
		},
		None => ()
	}
	
	let detailp = detail.find(None, FindOptions::builder().
		batch_size(10000).
		no_cursor_timeout(true).
		build()).unwrap();
	let mut total = 0;
	match detail.estimated_document_count(None) {
		Ok(count) => { 
			total = count;
		},
		Err(_) => {
		}
 	}
	let mut i = 0;
	let mut newdocs: Vec<Document> = Vec::new();
	let mut newtuples: Vec<(i32, String, i32)> = Vec::new();
	for result in detailp {
		match result {
			Ok(detaildoc) => {
			
				i += 1;
				// dot each 1000
				if (i % 1000) == 0 {
					print!(".");
 					io::stdout().flush().unwrap();
 					// flush the new docs.
					match newcollection {
						Some(c) => { let _ = c.insert_many(newdocs.drain(..), None); },
						None => { let _ = newdocs.drain(..); }
					}
				}
				// 80 columns :-) why not.
				if (i % 80000) == 0 {
					print!("({} tuples)", newtuples.len());
					println!(" {}%", (((i as f32) / (total as f32)) * 100.0).floor());
				}
				let (doc, tuple) = do_detail_gnaf(state, detaildoc, &street, &locality, &geo);
				match doc {
					Some(doc) => {
						newdocs.push(doc);
					},
					None => ()
				}
				match tuple {
					Some(tuple) => {
						if !newtuples.iter().any(|x| x.0 == tuple.0 && x.1 == tuple.1 && x.2 == tuple.2) {
							newtuples.push(tuple);
						}
					},
					None => ()
				}
			}
			Err(e) => {
				println!("err: {}", e);
			}
		}
	}
	
	// any remaining new docs.
	match newcollection {
		Some(c) => { let _ = c.insert_many(newdocs.drain(..), None); },
		None => { let _ = newdocs.drain(..); }
	}
 	
	println!("creating {} tuples", newtuples.len());
	match newtuplescollection {
		Some(c) => {
			let _ = c.insert_many(
						newtuples.drain(..).
						map(|x| doc! { "STATE": x.0, "LOCALITY": x.1, "ZIP": x.2 }), None);
		},
		None => ()
	}

	println!("");
}

fn build_one_gnaf(db: &Database, pid: &str, state: i32, name: &str, newcollection: &Collection, newtuple: &Collection) {
	println!("building {} in {} state {}", pid, name, state);
	_build_one_gnaf(pid, state,
		db.collection(format!("{}_address_detail", name).as_str()), 
		db.collection(format!("{}_street_locality", name).as_str()), 
		db.collection(format!("{}_locality", name).as_str()), 
		db.collection(format!("{}_address_default_geocode", name).as_str()), 
		&newcollection, &newtuple
	);
}

fn _build_one_gnaf(pid: &str, state: i32, detail: Collection, street: Collection, locality: Collection, geo: Collection, 
		newcollection: &Collection, newtuple: &Collection) {
		
	match newcollection.delete_one(doc! { "EID": pid }, DeleteOptions::builder().build()) {
		Ok(result) => { println!("deleted {} in new collection", result.deleted_count) },
		Err(e) => { println!("err: {}", e); }
	}
	match detail.find_one(doc! { "ADDRESS_DETAIL_PID": pid }, FindOneOptions::builder().build()) {
		Ok(detaildoc) => {
			match detaildoc {
				Some(doc) => {
					let (doc, tuple) = do_detail_gnaf(state, doc, &street, &locality, &geo);
					match doc {
						Some(doc) => {
							let _ = newcollection.insert_one(doc, None);
						},
						None => ()
					}
					match tuple {
						Some(tuple) => {
							let _ = newtuple.insert_one(doc! { "STATE": tuple.0, "LOCALITY": tuple.1, "ZIP": tuple.2 }, None);
						},
						None => ()
					}
				},
				None => {
					println!("doc not found.");
				}
			}
		}
		Err(e) => { println!("err: {}", e); }
	}
	
}

fn do_detail_gnaf(state: i32, detaildoc: Document, street: &Collection, locality: &Collection, geo: &Collection) -> 
		(Option<Document>, Option<(i32, String, i32)>) {
	
	let id = detail_id_gnaf(&detaildoc);
	match get_str(&detaildoc, &id, "STREET_LOCALITY_PID") {
		Some(sid) => {
			let streetdoc = get_street_doc(&street, sid.as_str());
			let geodoc = get_geo_doc(&geo, &id);
			
			match new_doc_gnaf(state, &detaildoc, &streetdoc, &geodoc) {
				Some(doc) => {
					match get_i32(&detaildoc, &id, "POSTCODE") {
						Some(zip) => { 
							match get_possible_str(&detaildoc, "LOCALITY_PID") {
								Some(lid) => {
									let locdoc = get_locality_doc(&locality, lid.as_str());
									match get_str(&locdoc, &id, "LOCALITY_NAME") {
										Some(locality) => {
											(Some(doc), Some((state, locality, zip)))
										},
										None => (Some(doc), None)
									}
								}
								None => {
									match get_i32(&detaildoc, &id, "LOCALITY_PID") {
										Some(lid) => {
											let locdoc = get_locality_doc_with_int(&locality, lid);
											match get_str(&locdoc, &id, "LOCALITY_NAME") {
												Some(locality) => {
													(Some(doc), Some((state, locality, zip)))
												}
												None => (Some(doc), None)
											}
										},
										None => (Some(doc), None)
									}
								}
							}
						},
						None => (Some(doc), None)
					}			
				},
				None => (None, None)
			}
		}
		None => {
			println!("missing STREET_LOCALITY_PID in detail {}, ", id);
			(None, None)
		}
	}
}

fn new_doc_gnaf(state: i32, detail: &Document, street: &Document, geo: &Document) -> Option<Document> {

	let id = detail_id_gnaf(&detail);
	let mut newdoc = doc! { "STATE": state, "EID": id.as_str() };
	match get_i32(&detail, &id, "POSTCODE") {
		Some(value) => { let _ = newdoc.insert("ZIP", value); },
		None => ()
	}
	match build_street(&street, "STREET_NAME", "STREET_TYPE_CODE", "STREET_SUFFIX_CODE") {
		Some(value) => { let _ = newdoc.insert("STREET", value); },
		None => { println!("missing street name in {}", street_id(&street)); }
	}
	
	match build_number_gnaf(&detail, "FLAT_NUMBER_PREFIX", "FLAT_NUMBER", "FLAT_NUMBER_SUFFIX") {
		Some(flatnum) => { 
			match get_str(&detail, &id, "FLAT_TYPE_CODE") {
				Some(flattype) => { let _ = newdoc.insert("UNIT", format!("{} {}", flattype, flatnum)); },
				None => { let _ = newdoc.insert("UNIT", flatnum); }
			}
		},
		None => ()
	}

	match build_number_gnaf(&detail, "LEVEL_NUMBER_PREFIX", "LEVEL_NUMBER", "LEVEL_NUMBER_SUFFIX") {
		Some(levelnum) => { 
			match get_str(&detail, &id, "LEVEL_TYPE_CODE") {
				Some(leveltype) => { let _ = newdoc.insert("LEVEL", format!("{} {}", leveltype, levelnum)); },
				None => { let _ = newdoc.insert("LEVEL", levelnum); }
			}
		},
		None => ()
	}
	
	match build_number_gnaf(&detail, "NUMBER_FIRST_PREFIX", "NUMBER_FIRST", "NUMBER_FIRST_SUFFIX") {
		Some(first) => { 
			match build_number_gnaf(&detail, "NUMBER_LAST_PREFIX", "NUMBER_LAST", "NUMBER_LAST_SUFFIX") {
				Some(last) => { 
					let _ = newdoc.insert("NUMBER", format!("{} - {}", first, last)); 
				},
				None => {
					let _ = newdoc.insert("NUMBER", first); 
				}
			}
		},
		None => ()
	}

	match build_number_gnaf(&detail, "LOT_NUMBER_PREFIX", "LOT_NUMBER", "LOT_NUMBER_SUFFIX") {
		Some(lot) => { 
			let _ = newdoc.insert("LOT", lot);
		}
		None => ()
	}
	match get_buildling_name(&detail, "BUILDING_NAME") {
		Some(value) => { let _ = newdoc.insert("BUILDING_NAME", value); },
		None => ()
	}
	match get_longlat_gnaf(&geo, "LONGITUDE") {
		Some(value) => { let _ = newdoc.insert("LONG", value); },
		None => ()
	}
	match get_longlat_gnaf(&geo, "LATITUDE\r") {
		Some(value) => { let _ = newdoc.insert("LAT", value); },
		None => ()
	}
	
	return Some(newdoc);	
}

fn detail_id_gnaf(detail: &Document) -> String {
	match detail.get("ADDRESS_DETAIL_PID").and_then(Bson::as_str) {
		Some(id) => {
			id.to_string()
		},
		None => {
			panic!("no ADDRESS_DETAIL_PID");
		}
	}
}

fn street_id(street: &Document) -> &str {
	street.get("STREET_LOCALITY_PID").and_then(Bson::as_str).unwrap()
}

fn build_number_gnaf(detail: &Document, prefix: &str, number: &str, suffix: &str) -> Option<String> {
	match detail.get(number).and_then(Bson::as_i32) {
		Some(num) => {
			match detail.get(suffix).and_then(Bson::as_str) {
				Some(sfx) => {
					match detail.get(prefix).and_then(Bson::as_str) {
						Some(pfx) => { Some(format!("{}{}{}", pfx, num, sfx)) },
						None => { Some(format!("{}{}", num, sfx)) }
					}
				},
				None => {
					match detail.get(prefix).and_then(Bson::as_str) {
						Some(pfx) => { Some(format!("{}{}", pfx, num)) },
						None => { Some(format!("{}", num)) }
					}
				}
			}
		},
		None => None
	}
}

fn build_with_suffix(street: &Document, suffix: &str, name: String) -> String {
	match street.get(suffix).and_then(Bson::as_str) {
		Some(s) => { 
			if s.len() > 0 {
				format!("{} {}", name, s)
			}
			else {
				name
			}
		},
		None => { 
			name
		}
	}
}

fn get_buildling_name(detail: &Document, name: &str) -> Option<String> {
	match detail.get(name).and_then(Bson::as_str) {
		Some(v) => { 
			if v.len() > 0 { 
				Some(v.to_string())
			}
			else {
				None
			}
		},
		None => { 
			// Wierd special case for a VIC address.
			match detail.get(name).and_then(Bson::as_f64) {
				Some(_n) => {
					Some("INFINITY".to_string())
				},
				None => { 
					println!("missing {} in {}", name, detail_id_gnaf(&detail)); 
					return None;
				}
			}
		}
	}
}

fn build_street(street: &Document, name: &str, code: &str, suffix: &str) -> Option<String> {
	match street.get(name).and_then(Bson::as_str) {
		Some(n) => {
			match street.get(code).and_then(Bson::as_str) {
				Some(c) => {
					Some(build_with_suffix(&street, suffix, format!("{} {}", n, c)))
				},
				None => { Some(n.to_string()) }
			}
		},
		None => { 
			// Wierd special case for a QLD address.
			match street.get(name).and_then(Bson::as_f64) {
				Some(_n) => {
					match street.get(code).and_then(Bson::as_str) {
						Some(c) => {
							Some(build_with_suffix(&street, suffix, format!("INFINITY {}", c)))
						},
						None => { Some("INFINITY".to_string()) }
					}
				},
				None => { None }
			}
		}
	}
}

fn get_i32(detail: &Document, id: &String, name: &str) -> Option<i32> {
	match detail.get(name).and_then(Bson::as_i32) {
		Some(v) => { Some(v) },
		None => { 
			println!("missing {} in {}", name, id);
			return None;
		}
	}
}

fn get_str(detail: &Document, id: &String, name: &str) -> Option<String> {
	match detail.get(name).and_then(Bson::as_str) {
		Some(v) => { 
			if v.len() > 0 { 
				Some(v.to_string())
			}
			else {
				None
			}
		},
		None => { 
			println!("missing {} in {}", name, id); 
			return None;
		}
	}
}

fn get_possible_str(detail: &Document, name: &str) -> Option<String> {
	match detail.get(name).and_then(Bson::as_str) {
		Some(v) => { 
			if v.len() > 0 { 
				Some(v.to_string())
			}
			else {
				None
			}
		},
		None => None
	}
}

fn get_longlat_gnaf(geo: &Document, name: &str) -> Option<f64> {
	match geo.get(name).and_then(Bson::as_str) {
		Some(v) => { 
			match f32::from_str(v.trim_end()) {
				Ok(d) => { Some(d as f64) }
				Err(e) => {
					println!("{} {} err: {}", detail_id_gnaf(&geo), name, e);
					return None;
				}
			}
		},
		None => { 
			match geo.get(name).and_then(Bson::as_f64) {
				Some(v) => { Some(v) },
				None => { 
					match geo.get(name).and_then(Bson::as_i32) {
						Some(v) => { Some(v as f64) },
						None => { 
							println!("missing {} in {}", name, detail_id_gnaf(&geo)); 
							return None;
						}
					}	
				}
			}	
		}
	}
}

fn get_street_doc(street: &Collection, sid: &str) -> Document {
	match street.find_one(doc! { "STREET_LOCALITY_PID": sid }, FindOneOptions::builder().build()) {
		Ok(doc) => {
			match doc {
				Some(d) => d,
				None => { panic!("unwrap get_street_doc {}. Maybe you didn't populate the DB?", sid); }
			}
		},
		Err(e) => {
			panic!("get_street_doc {} err: {}", sid, e);
		}
	}
}

fn get_locality_doc(locality: &Collection, id: &str) -> Document {
	match locality.find_one(doc! { "LOCALITY_PID": id }, FindOneOptions::builder().build()) {
		Ok(doc) => {
			match doc {
				Some(d) => d,
				None => { panic!("unwrap get_locality_doc {}. Maybe you didn't populate the DB?", id); }
			}
		},
		Err(e) => {
			panic!("get_locality_doc {} err: {}", id, e);
		}
	}
}

fn get_locality_doc_with_int(locality: &Collection, id: i32) -> Document {
	match locality.find_one(doc! { "LOCALITY_PID": id }, FindOneOptions::builder().build()) {
		Ok(doc) => {
			match doc {
				Some(d) => d,
				None => { panic!("unwrap get_locality_doc {}. Maybe you didn't populate the DB?", id); }
			}
		},
		Err(e) => {
			panic!("get_locality_doc {} err: {}", id, e);
		}
	}
}

fn get_geo_doc(geo: &Collection, id: &String) -> Document {
	match geo.find_one(doc! { "ADDRESS_DETAIL_PID": id }, FindOneOptions::builder().build()) {
		Ok(doc) => {
			match doc {
				Some(d) => d,
				None => { panic!("unwrap get_geo_doc {}. Maybe you didn't populate the DB?", id); }
			}
		},
		Err(e) => {
			panic!("get_geo_doc {} err: {}", id, e);
		}
	}
}

fn build_one_og(db: &Database, pid: &str, state: i32, newcollection: &Collection, newtuple: &Collection) {
	println!("building {} state {}", pid, state);
	_build_one_og(pid, state, db.collection("open_gov"), &newcollection, &newtuple);
}

fn _build_one_og(pid: &str, state: i32, detail: Collection, newcollection: &Collection, newtuple: &Collection) {
		
	match newcollection.delete_one(doc! { "EID": pid }, DeleteOptions::builder().build()) {
		Ok(result) => { println!("deleted {} in new collection", result.deleted_count) },
		Err(e) => { println!("err: {}", e); }
	}

	match f32::from_str(pid) {
		Ok(addr) => {
			match detail.find_one(doc! { "ADDRESS_ID": addr }, FindOneOptions::builder().build()) {
				Ok(detaildoc) => {
					match detaildoc {
						Some(doc) => {
							let (doc, tuple) = do_detail_og(state, doc);
							match doc {
								Some(doc) => {
									let _ = newcollection.insert_one(doc, None);
								},
								None => ()
							}
							match tuple {
								Some(tuple) => {
									let _ = newtuple.insert_one(doc! { "STATE": tuple.0, "LOCALITY": tuple.1, "ZIP": tuple.2 }, None);
								},
								None => ()
							}
						},
						None => {
							println!("doc not found.");
						}
					}
				}
				Err(e) => { println!("err: {}", e); }
			}
		},
		Err(e) => { println!("{} not a number err: {}", pid, e); }
	}
	
}

fn do_detail_og(state: i32, detaildoc: Document) -> (Option<Document>, Option<(i32, String, i32)>) {
	
	let id = detail_id_og(&detaildoc);
	match new_doc_og(state, &detaildoc) {
		Some(doc) => {
			match get_i32(&detaildoc, &id, "BOROCODE") {
				Some(lid) => {
					match get_i32(&detaildoc, &id, "ZIPCODE") {
						Some(zip) => { 
							(Some(doc), Some((state, i32::to_string(&lid), zip)))
						},
						None => (Some(doc), None)
					}			
				}
				None =>(Some(doc), None)
			}
		},
		None => (None, None)
	}

}

fn detail_id_og(detail: &Document) -> String {
	match detail.get("ADDRESS_ID").and_then(Bson::as_i32) {
		Some(id) => {
			i32::to_string(&id)
		},
		None => {
			panic!("no ADDRESS_ID");
		}
	}
}

fn new_doc_og(state: i32, detail: &Document) -> Option<Document> {

	let id = detail_id_og(&detail);
	let mut newdoc = doc! { "STATE": state, "EID": id.as_str() };
	match get_i32(&detail, &id, "ZIPCODE") {
		Some(value) => { let _ = newdoc.insert("ZIP", value); },
		None => { let _ = newdoc.insert("ZIP", 0); }
	}
	match get_str(&detail, &id, "FULL_STREE") {
		Some(value) => { let _ = newdoc.insert("STREET", value); },
		None => { println!("missing street name in {}", detail_id_og(&detail)); }
	}
	match build_number_og(&detail, "H_NO", "HNO_SUFFIX") {
		Some(number) => { 
			let _ = newdoc.insert("NUMBER", number); 
		},
		None => ()
	}
	match get_str(&detail, &id, "the_geom") {
		Some(value) => { 
			let (lat, long) = get_lat_long_og(value);
			let _ = newdoc.insert("LAT", lat);
			let _ = newdoc.insert("LONG", long);
		},
		None => { println!("missing the_gem in {}", detail_id_og(&detail)); }
	}
	
	return Some(newdoc);	
}

fn get_lat_long_og(point: String) -> (f64, f64) {
	match point.find("(") {
		Some(lbrack) => {
			let mut rem: String = point.chars().skip(lbrack + 1).collect();
			match rem.find(" ") {
				Some(space) => {
					let long: String = rem.chars().take(space).collect();
					rem = rem.chars().skip(space+1).collect();
					let lat: String = rem.chars().take(rem.len()-1).collect();
					match f64::from_str(lat.as_str()) {
						Ok(lt) => {
							match f64::from_str(long.as_str()) {
								Ok(ln) => (lt, ln),
								Err(_) => (0.0, 0.0)
							}
						},
						Err(_) => (0.0, 0.0)
					}
				},
				None => (0.0, 0.0)
			}
		},
		None => (0.0, 0.0)
	}
}

fn build_number_og(detail: &Document, number: &str, suffix: &str) -> Option<String> {
	match detail.get(number).and_then(Bson::as_i32) {
		Some(num) => {
			match detail.get(suffix).and_then(Bson::as_str) {
				Some(sfx) => {
					Some(format!("{}{}", num, sfx))
				},
				None => {
					Some(format!("{}", num))
				}
			}
		},
		None => None
	}
}

fn build_state_og(db: &Database, state: i32, newcollection: Option<&Collection>, newtuple: Option<&Collection>) {
	println!("building state {}", state);
	_build_state_og(state, db.collection("open_gov"), newcollection, newtuple);
}

fn _build_state_og(state: i32, detail: Collection, 
		newcollection: Option<&Collection>, newtuplescollection: Option<&Collection>) {
	
	match newcollection {
		Some(c) => {
			match c.delete_many(doc! { "STATE": state }, DeleteOptions::builder().build()) {
				Ok(result) => { println!("deleted {} in new collection", result.deleted_count) },
				Err(e) => { println!("err: {}", e); }
			}
		},
		None => ()
	}
	match newtuplescollection {
		Some(c) => {
			match c.delete_many(doc! { "STATE": state }, DeleteOptions::builder().build()) {
				Ok(result) => { println!("deleted {} in new tuples collection", result.deleted_count) },
				Err(e) => { println!("err: {}", e); }
			}
		},
		None => ()
	}
	
	let detailp = detail.find(None, FindOptions::builder().
		batch_size(10000).
		no_cursor_timeout(true).
		build()).unwrap();
	let mut total = 0;
	match detail.estimated_document_count(None) {
		Ok(count) => { 
			total = count;
		},
		Err(_) => {
		}
 	}
	let mut i = 0;
	let mut newdocs: Vec<Document> = Vec::new();
	let mut newtuples: Vec<(i32, String, i32)> = Vec::new();
	for result in detailp {
		match result {
			Ok(detaildoc) => {
			
				i += 1;
				// dot each 1000
				if (i % 1000) == 0 {
					print!(".");
 					io::stdout().flush().unwrap();
 					// flush the new docs.
					match newcollection {
						Some(c) => { let _ = c.insert_many(newdocs.drain(..), None); },
						None => { let _ = newdocs.drain(..); }
					}
				}
				// 80 columns :-) why not.
				if (i % 80000) == 0 {
					print!("({} tuples)", newtuples.len());
					println!(" {}%", (((i as f32) / (total as f32)) * 100.0).floor());
				}
				let (doc, tuple) = do_detail_og(state, detaildoc);
				match doc {
					Some(doc) => {
						newdocs.push(doc);
					},
					None => ()
				}
				match tuple {
					Some(tuple) => {
						if !newtuples.iter().any(|x| x.0 == tuple.0 && x.1 == tuple.1 && x.2 == tuple.2) {
							newtuples.push(tuple);
						}
					},
					None => ()
				}
			}
			Err(e) => {
				println!("err: {}", e);
			}
		}
	}
	
	// any remaining new docs.
	match newcollection {
		Some(c) => { let _ = c.insert_many(newdocs.drain(..), None); },
		None => { let _ = newdocs.drain(..); }
	}
 	
	println!("creating {} tuples", newtuples.len());
	match newtuplescollection {
		Some(c) => {
			let _ = c.insert_many(
						newtuples.drain(..).
						map(|x| doc! { "STATE": x.0, "LOCALITY": x.1, "ZIP": x.2 }), None);
		},
		None => ()
	}

	println!("");
}
