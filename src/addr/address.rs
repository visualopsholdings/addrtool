//! An address trait.
//!
//! Author: Paul Hamilton
//! Date: Wed 15 Apr, 2020
//!
//!

use mongodb::{ Collection };
use mongodb::options::{ FindOptions, DeleteOptions };
use bson::{ doc, Document };
use std::io::{ self, Write };
use structopt::StructOpt;
use std::string::String;

use super::cli::Cli;

pub trait Address {

	fn build_one(&self, pid: &str, state: i32, newcollection: &Collection, newtuple: &Collection);
	fn do_detail(&self, state: i32, detaildoc: Document) -> (Option<Document>, Option<(i32, String, i32)>);
	fn get_detail(&self) -> &Collection;
	
	fn process(&self, newcollection: &Collection, newtuplescollection: &Collection) {
	
    	let args = Cli::from_args();
   	
		if args.single.len() > 0 {
			self.build_one(args.single.as_str(), args.state, &newcollection, &newtuplescollection);
		}
		else {
			if args.nomain {
				if args.notuples {
					println!("Test run, nothing will happen");
					self.build_state(args.state, None, None);
				}
				else {
					println!("Only tuples.");
					self.build_state(args.state, None, Some(&newtuplescollection));
				}
			}
			if args.notuples {
				println!("Only main collection.");
				self.build_state(args.state, Some(&newcollection), None);
			}
			else {
				self.build_state(args.state, Some(&newcollection), Some(&newtuplescollection));
			}
		}
	}
	
	fn build_state(&self, state: i32, newcollection: Option<&Collection>, newtuplescollection: Option<&Collection>) {
	
		println!("building {} state {}", self.get_detail().name(), state);
		
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
	
		let detailp = self.get_detail().find(None, FindOptions::builder().
			batch_size(10000).
			no_cursor_timeout(true).
			build()).unwrap();
		let mut total = 0;
		match self.get_detail().estimated_document_count(None) {
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
					let (doc, tuple) = self.do_detail(state, detaildoc);
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
}
