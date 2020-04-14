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

/// Import tool for the G-NAF australian address database.
#[derive(StructOpt)]
struct Cli {

    #[structopt(help="The name of the DB in MongoDB")]
    db: String,
    
    #[structopt(help="The prefix for the various G-NAF to use")]
    coll: String,
    
    #[structopt(help="An identifier for the STATE field")]
    state: i32,
    
    #[structopt(help="The name of the collection to create")]
    name: String,
    
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

	if args.single.len() > 0 {
		build_one(&db, args.single.as_str(), args.state, args.name.as_str(), &newcollection, &newtuple);
	}
	else {
		if args.nomain {
			if args.notuples {
				println!("Test run, nothing will happen");
				build_state(&db, args.state, args.name.as_str(), None, None);
			}
			else {
				println!("Only tuples.");
				build_state(&db, args.state, args.name.as_str(), None, Some(&newtuple));
			}
		}
		if args.notuples {
			println!("Only main collection.");
			build_state(&db, args.state, args.name.as_str(), Some(&newcollection), None);
		}
		else {
			build_state(&db, args.state, args.name.as_str(), Some(&newcollection), Some(&newtuple));
		}
	}
}

fn build_state(db: &Database, state: i32, name: &str, newcollection: Option<&Collection>, newtuple: Option<&Collection>) {
	println!("building {} state {}", name, state);
	_build_state(state, 
		db.collection(format!("{}_address_detail", name).as_str()), 
		db.collection(format!("{}_street_locality", name).as_str()), 
		db.collection(format!("{}_locality", name).as_str()), 
		db.collection(format!("{}_address_default_geocode", name).as_str()), 
		newcollection, newtuple
	);
}

fn _build_state(state: i32, detail: Collection, street: Collection, locality: Collection, geo: Collection, 
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
				let (doc, tuple) = do_detail(state, detaildoc, &street, &locality, &geo);
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

fn build_one(db: &Database, pid: &str, state: i32, name: &str, newcollection: &Collection, newtuple: &Collection) {
	println!("building {} in {} state {}", pid, name, state);
	_build_one(pid, state,
		db.collection(format!("{}_address_detail", name).as_str()), 
		db.collection(format!("{}_street_locality", name).as_str()), 
		db.collection(format!("{}_locality", name).as_str()), 
		db.collection(format!("{}_address_default_geocode", name).as_str()), 
		&newcollection, &newtuple
	);
}

fn _build_one(pid: &str, state: i32, detail: Collection, street: Collection, locality: Collection, geo: Collection, 
		newcollection: &Collection, newtuple: &Collection) {
		
	match newcollection.delete_one(doc! { "EID": pid }, DeleteOptions::builder().build()) {
		Ok(result) => { println!("deleted {} in new collection", result.deleted_count) },
		Err(e) => { println!("err: {}", e); }
	}
	match detail.find_one(doc! { "ADDRESS_DETAIL_PID": pid }, FindOneOptions::builder().build()) {
		Ok(detaildoc) => {
			match detaildoc {
				Some(doc) => {
					let (doc, tuple) = do_detail(state, doc, &street, &locality, &geo);
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
fn do_detail(state: i32, detaildoc: Document, street: &Collection, locality: &Collection, geo: &Collection) -> 
		(Option<Document>, Option<(i32, String, i32)>) {
	
	let detailid = detail_id(&detaildoc);
	match get_str(&detaildoc, "STREET_LOCALITY_PID") {
		Some(sid) => {
			let streetdoc = get_street_doc(&street, sid.as_str());
			let geodoc = get_geo_doc(&geo, detailid);
			
			match new_doc(state, &detaildoc, &streetdoc, &geodoc) {
				Some(doc) => {
					match get_i32(&detaildoc, "POSTCODE") {
						Some(zip) => { 
							match get_possible_str(&detaildoc, "LOCALITY_PID") {
								Some(lid) => {
									let locdoc = get_locality_doc(&locality, lid.as_str());
									match get_str(&locdoc, "LOCALITY_NAME") {
										Some(locality) => {
											(Some(doc), Some((state, locality, zip)))
										},
										None => (None, None)
									}
								}
								None => {
									match get_i32(&detaildoc, "LOCALITY_PID") {
										Some(lid) => {
											let locdoc = get_locality_doc_with_int(&locality, lid);
											match get_str(&locdoc, "LOCALITY_NAME") {
												Some(locality) => {
													(Some(doc), Some((state, locality, zip)))
												}
												None => (None, None)
											}
										},
										None => (None, None)
									}
								}
							}
						},
						None => (None, None)
					}			
				},
				None => (None, None)
			}
		}
		None => {
			println!("missing STREET_LOCALITY_PID in detail {}, ", detailid);
			(None, None)
		}
	}
}

fn new_doc(state: i32, detail: &Document, street: &Document, geo: &Document) -> Option<Document> {

	let mut newdoc = doc! { "STATE": state };
	match get_str(&detail, "ADDRESS_DETAIL_PID") {
		Some(value) => { let _ = newdoc.insert("EID", value); },
		None => ()
	}
	match get_i32(&detail, "POSTCODE") {
		Some(value) => { let _ = newdoc.insert("ZIP", value); },
		None => ()
	}
	match build_street(&street, "STREET_NAME", "STREET_TYPE_CODE", "STREET_SUFFIX_CODE") {
		Some(value) => { let _ = newdoc.insert("STREET", value); },
		None => { println!("missing street name in {}", street_id(&street)); }
	}
	
	match build_number(&detail, "FLAT_NUMBER_PREFIX", "FLAT_NUMBER", "FLAT_NUMBER_SUFFIX") {
		Some(flatnum) => { 
			match get_str(&detail, "FLAT_TYPE_CODE") {
				Some(flattype) => { let _ = newdoc.insert("UNIT", format!("{} {}", flattype, flatnum)); },
				None => { let _ = newdoc.insert("UNIT", flatnum); }
			}
		},
		None => ()
	}

	match build_number(&detail, "LEVEL_NUMBER_PREFIX", "LEVEL_NUMBER", "LEVEL_NUMBER_SUFFIX") {
		Some(levelnum) => { 
			match get_str(&detail, "LEVEL_TYPE_CODE") {
				Some(leveltype) => { let _ = newdoc.insert("LEVEL", format!("{} {}", leveltype, levelnum)); },
				None => { let _ = newdoc.insert("LEVEL", levelnum); }
			}
		},
		None => ()
	}
	
	match build_number(&detail, "NUMBER_FIRST_PREFIX", "NUMBER_FIRST", "NUMBER_FIRST_SUFFIX") {
		Some(first) => { 
			match build_number(&detail, "NUMBER_LAST_PREFIX", "NUMBER_LAST", "NUMBER_LAST_SUFFIX") {
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

	match build_number(&detail, "LOT_NUMBER_PREFIX", "LOT_NUMBER", "LOT_NUMBER_SUFFIX") {
		Some(lot) => { 
			let _ = newdoc.insert("LOT", lot);
		}
		None => ()
	}
	match get_buildling_name(&detail, "BUILDING_NAME") {
		Some(value) => { let _ = newdoc.insert("BUILDING_NAME", value); },
		None => ()
	}
	match get_longlat(&geo, "LONGITUDE") {
		Some(value) => { let _ = newdoc.insert("LONG", value); },
		None => ()
	}
	match get_longlat(&geo, "LATITUDE\r") {
		Some(value) => { let _ = newdoc.insert("LAT", value); },
		None => ()
	}
	
//	let _ = newcollection.insert_one(newdoc, None);
	return Some(newdoc);	
}

fn detail_id(detail: &Document) -> &str {
	detail.get("ADDRESS_DETAIL_PID").and_then(Bson::as_str).unwrap()
}

fn street_id(street: &Document) -> &str {
	street.get("STREET_LOCALITY_PID").and_then(Bson::as_str).unwrap()
}

fn build_number(detail: &Document, prefix: &str, number: &str, suffix: &str) -> Option<String> {
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
					println!("missing {} in {}", name, detail_id(&detail)); 
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

fn get_i32(detail: &Document, name: &str) -> Option<i32> {
	match detail.get(name).and_then(Bson::as_i32) {
		Some(v) => { Some(v) },
		None => { 
			println!("missing {} in {}", name, detail_id(&detail));
			return None;
		}
	}
}

fn get_str(detail: &Document, name: &str) -> Option<String> {
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
			println!("missing {} in {}", name, detail_id(&detail)); 
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

fn get_longlat(geo: &Document, name: &str) -> Option<f64> {
	match geo.get(name).and_then(Bson::as_str) {
		Some(v) => { 
			match f32::from_str(v.trim_end()) {
				Ok(d) => { Some(d as f64) }
				Err(e) => {
					println!("{} {} err: {}", detail_id(&geo), name, e);
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
							println!("missing {} in {}", name, detail_id(&geo)); 
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

fn get_geo_doc(geo: &Collection, id: &str) -> Document {
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

