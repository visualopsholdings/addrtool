//! This is my first very rust program.
//!
//! Author: Paul Hamilton
//! Date: Wed 15 Apr, 2020
//!

use mongodb::{ Client, options::ClientOptions };
use structopt::StructOpt;

pub mod addr;

use addr::address::Address;

fn main() {

    let args = addr::cli::Cli::from_args();
    
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
	
		let a = addr::gnaf::Gnaf {
			detail: db.collection(format!("{}_address_detail", args.gnaf.as_str()).as_str()),
			street: db.collection(format!("{}_street_locality", args.gnaf.as_str()).as_str()),
			locality: db.collection(format!("{}_locality", args.gnaf.as_str()).as_str()),
			geo: db.collection(format!("{}_address_default_geocode", args.gnaf.as_str()).as_str())
		};
		a.process(&newcollection, &newtuple);
	}
	else if args.opengov {
	
		let a = addr::opengov::Opengov {
			detail: db.collection("open_gov")
		};
		a.process(&newcollection, &newtuple);
	}
	else {
		println!("Need to set --opengov or --gnaf prefix.")
	}
}
