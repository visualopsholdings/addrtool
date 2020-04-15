//! G-NAF address processor.
//!
//! Author: Paul Hamilton
//! Date: Wed 15 Apr, 2020
//!
//!

use mongodb::{ Collection };
use mongodb::options::{ FindOneOptions, DeleteOptions };
use bson::{ doc, Bson, Document };
use std::str::FromStr;
use std::string::String;

use super::address::Address;

pub struct Gnaf {
	pub detail: Collection,
	pub street: Collection, 
	pub locality: Collection,
	pub geo: Collection
}

impl Address for Gnaf {

	fn get_detail(&self) -> &Collection {
		&self.detail
	}
	
	fn do_detail(&self, state: i32, detaildoc: Document) -> (Option<Document>, Option<(i32, String, i32)>) {
	
		let id = detail_id_gnaf(&detaildoc);
		match get_str(&detaildoc, &id, "STREET_LOCALITY_PID") {
			Some(sid) => {
				let streetdoc = get_street_doc(&self.street, sid.as_str());
				let geodoc = get_geo_doc(&self.geo, &id);
			
				match new_doc_gnaf(state, &detaildoc, &streetdoc, &geodoc) {
					Some(doc) => {
						match get_i32(&detaildoc, &id, "POSTCODE") {
							Some(zip) => { 
								match get_possible_str(&detaildoc, "LOCALITY_PID") {
									Some(lid) => {
										let locdoc = get_locality_doc(&self.locality, lid.as_str());
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
												let locdoc = get_locality_doc_with_int(&self.locality, lid);
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
	
	fn build_one(&self, pid: &str, state: i32, newcollection: &Collection, newtuplescollection: &Collection) {
	
		println!("building {} in {} state {}", pid, self.detail.name(), state);
		
		match newcollection.delete_one(doc! { "EID": pid }, DeleteOptions::builder().build()) {
			Ok(result) => { println!("deleted {} in new collection", result.deleted_count) },
			Err(e) => { println!("err: {}", e); }
		}
		match self.detail.find_one(doc! { "ADDRESS_DETAIL_PID": pid }, FindOneOptions::builder().build()) {
			Ok(detaildoc) => {
				match detaildoc {
					Some(doc) => {
						let (doc, tuple) = self.do_detail(state, doc);
						match doc {
							Some(doc) => {
								let _ = newcollection.insert_one(doc, None);
							},
							None => ()
						}
						match tuple {
							Some(tuple) => {
								let _ = newtuplescollection.insert_one(
										doc! { "STATE": tuple.0, "LOCALITY": tuple.1, "ZIP": tuple.2 }, None);
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
