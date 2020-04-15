//! The CLI args.
//!
//! Author: Paul Hamilton
//! Date: Wed 15 Apr, 2020
//!
//!

use structopt::StructOpt;

/// Import tool for the either G-NAF australian address database or US open gov address data.
#[derive(StructOpt)]
pub struct Cli {

    #[structopt(help="The name of the DB in MongoDB")]
    pub db: String,
    
    #[structopt(long, help="G-NAF data", default_value="")]
    pub gnaf: String,

    #[structopt(long, help="US Open Government data")]
    pub opengov: bool,

    #[structopt(help="The name of the collection to create")]
    pub coll: String,
    
    #[structopt(help="An identifier for the STATE field", default_value="1")]
    pub state: i32,
    
    #[structopt(long, default_value="", help="The ID of a single record to process")]
    pub single: String,
	
    #[structopt(long, help="Drop the new collections before proceeding")]
    pub drop: bool,
	
    #[structopt(long, help="Skip creating the main collection")]
    pub nomain: bool,
	
    #[structopt(long, help="Skip creating the tuples collection")]
    pub notuples: bool
	
}
