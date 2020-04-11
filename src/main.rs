//! This is my first very rust program.
//!
//! Be kind.
//!
//!

use mongodb::{ Database, Client, options::ClientOptions, Collection };
use mongodb::options::{ FindOptions, FindOneOptions };
use bson::{ doc, Bson, Document };
use std::io::{ self, Write };
use std::str::FromStr;

fn main() {

	let client_options = ClientOptions::parse("mongodb://localhost:27017").unwrap();
	let client = Client::with_options(client_options).unwrap();
	// set your DB name here.
	let db = client.database("fiveEstellas");
	
	// whatever you want the new DB to be called.
	let newcollection = db.collection("aus");
	let _ = newcollection.drop(None);
	
	// test on one record.
//	test("GAQLD161695239", 5, db.collection("qld_address_detail"), db.collection("qld_street_locality"), db.collection("qld_locality"), db.collection("qld_address_default_geocode"), &newcollection);

	// let it rip.
	build_state(&db, 1, "act", &newcollection);
	build_state(&db, 2, "nsw", &newcollection);
	build_state(&db, 3, "nt", &newcollection);
	build_state(&db, 4, "ot", &newcollection);
	build_state(&db, 5, "qld", &newcollection);
	build_state(&db, 6, "sa", &newcollection);
	build_state(&db, 7, "tas", &newcollection);
	build_state(&db, 8, "vic", &newcollection);
	build_state(&db, 9, "wa", &newcollection);
}

fn build_state(db: &Database, state: i32, name: &str, newcollection: &Collection) {
	_build_state(state, 
		db.collection(format!("{}_address_detail", name).as_str()), 
		db.collection(format!("{}_street_locality", name).as_str()), 
		db.collection(format!("{}_locality", name).as_str()), 
		db.collection(format!("{}_address_default_geocode", name).as_str()), 
		&newcollection);
}

// fn test(pid: &str, state: i32, detail: Collection, street: Collection, locality: Collection, geo: Collection, newcollection: &Collection) {
// 	match detail.find_one(doc! { "ADDRESS_DETAIL_PID": pid }, FindOneOptions::builder().build()) {
// 		Ok(detaildoc) => {
// 			do_detail(state, detaildoc.unwrap(), &street, &locality, &geo, &newcollection)
// 		}
// 		Err(e) => {
// 			println!("err: {}", e);
// 		}
// 	}
// }

fn _build_state(state: i32, detail: Collection, street: Collection, locality: Collection, geo: Collection, newcollection: &Collection) {
	
	println!("state {}", state);
	let detailp = detail.find(None, FindOptions::builder().build()).unwrap();
	let mut i = 0;
	for result in detailp {
		match result {
			Ok(detaildoc) => {
			
				i += 1;
				// dot each 1000
				if (i % 1000) == 0 {
					print!(".");
					io::stdout().flush().unwrap();
				}
				// 80 columns :-) why not.
				if (i % 80000) == 0 {
					println!("");
				}
				
				do_detail(state, detaildoc, &street, &locality, &geo, &newcollection)
			}
			Err(e) => {
				println!("err: {}", e);
			}
		}
	}
	println!("");
}

fn do_detail(state: i32, detaildoc: Document, street: &Collection, locality: &Collection, geo: &Collection, newcollection: &Collection) {
	
	let detailid = detail_id(&detaildoc);
	match detaildoc.get("STREET_LOCALITY_PID").and_then(Bson::as_str) {
		Some(sid) => {
			let streetdoc = get_street_doc(&street, sid);
			let geodoc = get_geo_doc(&geo, detailid);
			match detaildoc.get("LOCALITY_PID").and_then(Bson::as_str) {
				Some(id) => {
					let locdoc = get_locality_doc(&locality, id);
					match locdoc.get("LOCALITY_NAME").and_then(Bson::as_str) {
						Some(name) => {
							new_doc(state, &detaildoc, &streetdoc, &geodoc, name, &newcollection)
						}
						None => {
							new_doc(state, &detaildoc, &streetdoc, &geodoc, "UNKNOWN", &newcollection)
						}
					}
				}
				None => {
					new_doc(state, &detaildoc, &streetdoc, &geodoc, "UNKNOWN", &newcollection)
				}
			}
		}
		None => {
			println!("missing STREET_LOCALITY_PID in detail {}, ", detailid);
		}
	}
}

fn detail_id(detail: &Document) -> &str {
	detail.get("ADDRESS_DETAIL_PID").and_then(Bson::as_str).unwrap()
}

fn street_id(street: &Document) -> &str {
	street.get("STREET_LOCALITY_PID").and_then(Bson::as_str).unwrap()
}

fn build_number(detail: &Document, prefix: &str, number: &str, suffix: &str, key: &str, newdoc: &mut Document) {
	match detail.get(number).and_then(Bson::as_i32) {
		Some(num) => {
			match detail.get(suffix).and_then(Bson::as_str) {
				Some(sfx) => {
					match detail.get(prefix).and_then(Bson::as_str) {
						Some(pfx) => { let _ = newdoc.insert(key, format!("{}{}{}", pfx, num, sfx)); },
						None => { let _ = newdoc.insert(key, format!("{}{}", num, sfx)); }
					}
				},
				None => {
					match detail.get(prefix).and_then(Bson::as_str) {
						Some(pfx) => { let _ = newdoc.insert(key, format!("{}{}", pfx, num)); },
						None => { let _ = newdoc.insert(key, format!("{}", num)); }
					}
				}
			}
		},
		None => ()
	}
}

fn build_street(street: &Document, name: &str, code: &str, suffix: &str, key: &str, newdoc: &mut Document) {
	match street.get(name).and_then(Bson::as_str) {
		Some(n) => {
			match street.get(code).and_then(Bson::as_str) {
				Some(c) => {
					match street.get(suffix).and_then(Bson::as_str) {
						Some(s) => { let _ = newdoc.insert(key, format!("{} {} {}", n, c, s)); },
						None => { let _ = newdoc.insert(key, format!("{} {}", n, c)); }
					}
				},
				None => { let _ = newdoc.insert(key, n); }
			}
		},
		None => { 
			// Wierd special case for a QLD address.
			match street.get(name).and_then(Bson::as_f64) {
				Some(_n) => {
					match street.get(code).and_then(Bson::as_str) {
						Some(c) => {
							match street.get(suffix).and_then(Bson::as_str) {
								Some(s) => { let _ = newdoc.insert(key, format!("INFINITY {} {}", c, s)); },
								None => { let _ = newdoc.insert(key, format!("INFINITY {}", c)); }
							}
						},
						None => { let _ = newdoc.insert(key, "INFINITY"); }
					}
				},
				None => { println!("missing street name {} in {}", name, street_id(&street)); }
			}
		}
	}
}

fn build_i32(detail: &Document, name: &str, newname: &str, newdoc: &mut Document) {
	match detail.get(name).and_then(Bson::as_i32) {
		Some(v) => { let _ = newdoc.insert(newname, v); },
		None => { println!("missing {} in {}", name, detail_id(&detail)); }
	}
}

fn build_str(detail: &Document, name: &str, newname: &str, newdoc: &mut Document) {
	match detail.get(name).and_then(Bson::as_str) {
		Some(v) => { if v.len() > 0 { let _ = newdoc.insert(newname, v); } },
		None => { println!("missing {} in {}", name, detail_id(&detail)); }
	}
}

fn build_longlat(geo: &Document, name: &str, newname: &str, newdoc: &mut Document) {
	match geo.get(name).and_then(Bson::as_str) {
		Some(v) => { 
			match f32::from_str(v.trim_end()) {
				Ok(d) => {
					{ let _ = newdoc.insert(newname, d); }
				}
				Err(e) => {
					println!("{} {} err: {}", detail_id(&geo), name, e);
				}
			}
		},
		None => { 
			match geo.get(name).and_then(Bson::as_f64) {
				Some(v) => { let _ = newdoc.insert(newname, v); },
				None => { println!("missing {} in {}", name, detail_id(&geo)); }
			}	
		}
	}
}

fn new_doc(state: i32, detail: &Document, street: &Document, geo: &Document, locality: &str, newcollection: &Collection) {
	let mut newdoc = doc! { 
		"STATE": state,
		"LOCALITY": locality
	};
	build_str(&detail, "ADDRESS_DETAIL_PID", "ADDRESS_DETAIL_PID", &mut newdoc);
	build_i32(&detail, "POSTCODE", "POSTCODE", &mut newdoc);
	build_str(&detail, "FLAT_TYPE_CODE", "FLAT_TYPE", &mut newdoc);
	build_str(&detail, "LEVEL_TYPE_CODE", "LEVEL_TYPE", &mut newdoc);
	build_street(&street, "STREET_NAME", "STREET_TYPE_CODE", "STREET_SUFFIX_CODE", "STREET", &mut newdoc);
	build_number(&detail, "FLAT_NUMBER_PREFIX", "FLAT_NUMBER", "FLAT_NUMBER_SUFFIX", "FLAT_NUMBER", &mut newdoc);
	build_number(&detail, "LEVEL_NUMBER_PREFIX", "LEVEL_NUMBER", "LEVEL_NUMBER_SUFFIX", "LEVEL_NUMBER", &mut newdoc);
	build_number(&detail, "NUMBER_FIRST_PREFIX", "NUMBER_FIRST", "NUMBER_FIRST_SUFFIX", "NUMBER_FIRST", &mut newdoc);
	build_number(&detail, "NUMBER_LAST_PREFIX", "NUMBER_LAST", "NUMBER_LAST_SUFFIX", "NUMBER_LAST", &mut newdoc);
	build_longlat(&geo, "LONGITUDE", "LONG", &mut newdoc);
	build_longlat(&geo, "LATITUDE\r", "LAT", &mut newdoc);
	let _ = newcollection.insert_one(newdoc, None);
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

