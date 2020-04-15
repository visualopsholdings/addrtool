//! OpenGov address processor.
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

pub struct Opengov {
	pub detail: Collection
}

impl Address for Opengov {

	fn get_detail(&self) -> &Collection {
		&self.detail
	}
	
	fn do_detail(&self, state: i32, detaildoc: Document) -> (Option<Document>, Option<(i32, String, i32)>) {
	
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
	
	fn build_one(&self, pid: &str, state: i32, newcollection: &Collection, newtuplescollection: &Collection) {
	
		println!("building {} in {} state {}", pid, self.detail.name(), state);
		
		match newcollection.delete_one(doc! { "EID": pid }, DeleteOptions::builder().build()) {
			Ok(result) => { println!("deleted {} in new collection", result.deleted_count) },
			Err(e) => { println!("err: {}", e); }
		}

		match f32::from_str(pid) {
			Ok(addr) => {
				match self.detail.find_one(doc! { "ADDRESS_ID": addr }, FindOneOptions::builder().build()) {
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
										let _ = newtuplescollection.insert_one(doc! { "STATE": tuple.0, "LOCALITY": tuple.1, "ZIP": tuple.2 }, None);
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

