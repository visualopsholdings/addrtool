use mongodb::{ Client, options::ClientOptions, Collection };
use mongodb::options::{ FindOptions, FindOneOptions };
use mongodb::error::{ Result };
use bson::{ doc, Bson, Document };
use std::io::{ self, Write };
use std::str::FromStr;

fn main() {

	let client_options = ClientOptions::parse("mongodb://localhost:27017").unwrap();
	let client = Client::with_options(client_options).unwrap();
	let db = client.database("fiveEstellas");
	
	let newcollection = db.collection("aus");
	let _ = newcollection.drop(None);
	
//	build_state(1, db.collection("act_address_detail"), db.collection("act_street_locality"), db.collection("act_locality", db.collection("act_address_default_geocode"), &newcollection);
//	build_state(2, db.collection("nsw_address_detail"), db.collection("nsw_street_locality"), db.collection("nsw_locality"), db.collection("nsw_address_default_geocode"), &newcollection);
//	build_state(3, db.collection("nt_address_detail"), db.collection("nt_street_locality"), db.collection("nt_locality"), db.collection("nt_address_default_geocode"), &newcollection);
//	build_state(4, db.collection("ot_address_detail"), db.collection("ot_street_locality"), db.collection("ot_locality"), db.collection("ot_address_default_geocode"), &newcollection);
	build_state(5, db.collection("qld_address_detail"), db.collection("qld_street_locality"), db.collection("qld_locality"), db.collection("qld_address_default_geocode"), &newcollection);
	build_state(6, db.collection("sa_address_detail"), db.collection("sa_street_locality"), db.collection("sa_locality"), db.collection("sa_address_default_geocode"), &newcollection);
	build_state(7, db.collection("tas_address_detail"), db.collection("tas_street_locality"), db.collection("tas_locality"), db.collection("tas_address_default_geocode"), &newcollection);
	build_state(8, db.collection("vic_address_detail"), db.collection("vic_street_locality"), db.collection("vic_locality"), db.collection("vic_address_default_geocode"), &newcollection);
	build_state(9, db.collection("wa_address_detail"), db.collection("wa_street_locality"), db.collection("wa_locality"), db.collection("wa_address_default_geocode"), &newcollection);
//	test(5, db.collection("qld_address_detail"), db.collection("qld_street_locality"), db.collection("qld_locality"), &newcollection)
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
	newcollection.insert_one(newdoc, None).unwrap();
}

fn get_street_doc(street: &Collection, sid: &str) -> Result<Option<Document> > {
	street.find_one(doc! { "STREET_LOCALITY_PID": sid }, FindOneOptions::builder().build())
}

fn get_locality_doc(locality: &Collection, id: &str) -> Result<Option<Document> > {
	locality.find_one(doc! { "LOCALITY_PID": id }, FindOneOptions::builder().build())
}

fn get_geo_doc(geo: &Collection, id: &str) -> Result<Option<Document> > {
	geo.find_one(doc! { "ADDRESS_DETAIL_PID": id }, FindOneOptions::builder().build())
}

// fn test(state: i32, detail: Collection, street: Collection, locality: Collection, newcollection: &Collection) {
// 	match detail.find_one(doc! { "ADDRESS_DETAIL_PID": "GAQLD161695239"}, FindOneOptions::builder().build()) {
// 		Ok(detaildoc) => {
// 			do_detail(state, detaildoc.unwrap(), &street, &locality, &newcollection)
// 		}
// 		Err(e) => {
// 			println!("err: {}", e);
// 		}
// 	}
// }

fn do_detail(state: i32, detaildoc: Document, street: &Collection, locality: &Collection, geo: &Collection, newcollection: &Collection) {
	match detaildoc.get("STREET_LOCALITY_PID").and_then(Bson::as_str) {
		Some(sid) => {
			match get_street_doc(&street, sid) {
				Ok(streetdoc) => {
					match detaildoc.get("LOCALITY_PID").and_then(Bson::as_str) {
						Some(id) => {
							let detailid = detail_id(&detaildoc);
							match get_locality_doc(&locality, id) {
								Ok(locdoc) => {
									match get_geo_doc(&geo, detailid) {
										Ok(geodoc) => {
											new_doc(state, &detaildoc, &streetdoc.unwrap(), &geodoc.unwrap(), locdoc.unwrap().get("LOCALITY_NAME").and_then(Bson::as_str).unwrap(), &newcollection)
										}
										Err(e) => {
											println!("geo {} err: {}", detailid, e);
										}
									}
								}
								Err(e) => {
									println!("{} err: {}", detailid, e);
								}
							}
						}
						None => {
							let detailid = detail_id(&detaildoc);
							match get_geo_doc(&geo, detailid) {
								Ok(geodoc) => {
									new_doc(state, &detaildoc, &streetdoc.unwrap(), &geodoc.unwrap(), "UNKNOWN", &newcollection)
								}
								Err(e) => {
									println!("noloc geo {} err: {}", detailid, e);
								}
							}
						}
					}
				}
				Err(e) => {
					println!("{} {} err: {}", detail_id(&detaildoc), sid, e);
				}
			}
		}
		None => {
			println!("missing STREET_LOCALITY_PID in detail {}, ", detail_id(&detaildoc));
		}
	}
}

fn build_state(state: i32, detail: Collection, street: Collection, locality: Collection, geo: Collection, newcollection: &Collection) {
	
	println!("state {}", state);
	let detailp = detail.find(None, FindOptions::builder().build()).unwrap();

	let mut i = 0;
	for result in detailp {
		match result {
			Ok(detaildoc) => {
			
				i += 1;
				if (i % 100) == 0 {
					print!(".");
					io::stdout().flush().unwrap();
				}
				if (i % 5000) == 0 {
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
