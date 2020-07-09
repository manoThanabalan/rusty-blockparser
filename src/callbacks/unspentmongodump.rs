use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::{App, Arg, ArgMatches, SubCommand};

use mongodb::{
    bson::{doc, Bson},
    sync::Client, sync::Collection
};
use crate::callbacks::Callback;
use crate::errors::{OpError, OpResult};

use crate::blockchain::parser::types::CoinType;
use crate::blockchain::proto::block::Block;
use crate::blockchain::utils;


/// Dumps the whole blockchain into csv files
pub struct UnspentMongoDump {
    // Each structure gets stored in a seperate csv file
    collection: OpResult<Collection>,
    transactions_unspent: HashMap<String, HashMapVal>,
    start_height: usize,
    end_height: usize,
    tx_count: u64,
    in_count: u64,
    out_count: u64,
}

struct HashMapVal {
    /*	txid:	String,
    index:	usize,*/
    block_height: usize,
    output_val: u64,
    address: String,
}

impl UnspentMongoDump {
    fn get_collection() -> OpResult<Collection> {
        let client = match Client::with_uri_str("mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/bitcoindb?replicaSet=devrs") {
            Ok(c) => c,
            Err(err) => return Err(OpError::from(err)),
        };

        let db = client.database("bitcoindb");
        let coll = db.collection("utxos");
        return Ok(coll);
    }
}

impl Callback for UnspentMongoDump {
    fn build_subcommand<'a, 'b>() -> App<'a, 'b>
    where
        Self: Sized,
    {
        SubCommand::with_name("unspentmongodump")
            .about("Dumps the unspent outputs to mongodb")
            .version("0.1")
            .author("mano <mano.thanabalan@gmail.com>")
            .arg(
                Arg::with_name("db-name")
                    .help("Database to store in")
                    .index(1)
                    .required(true),
            )
    }

    fn new(matches: &ArgMatches) -> OpResult<Self>
    where
        Self: Sized,
    {
        match (|| -> OpResult<Self> {
            let cap = 4000000;
            let cb = UnspentMongoDump {
                collection: UnspentMongoDump::get_collection(),
                transactions_unspent: HashMap::with_capacity(10000000), // Init hashmap for tracking the unspent transactions (with 10'000'000 mln preallocated entries)
                start_height: 0,
                end_height: 0,
                tx_count: 0,
                in_count: 0,
                out_count: 0,
            };
            Ok(cb)
        })() {
            Ok(s) => Ok(s),
            Err(e) => Err(tag_err!(
                e,
                "Couldn't initialize mongo db",
            )),
        }
    }

    fn on_start(&mut self, _: CoinType, block_height: usize) {
        self.start_height = block_height;
        info!(target: "callback", "Using `UnspentMongoDump`");
    }

    fn on_block(&mut self, block: Block, block_height: usize) {
        // serialize transaction
        for tx in block.txs {
            // For each transaction in the block,
            // 1. apply input transactions (remove (TxID == prevTxIDOut and prevOutID == spentOutID))
            // 2. apply output transactions (add (TxID + curOutID -> HashMapVal))
            // For each address, retain:
            // * block height as "last modified"
            // * output_val
            // * address

            //self.tx_writer.write_all(tx.as_csv(&block_hash).as_bytes()).unwrap();
            let txid_str = utils::arr_to_hex_swapped(&tx.hash);

            for input in &tx.value.inputs {
                let input_outpoint_txid_idx = utils::arr_to_hex_swapped(&input.outpoint.txid)
                    + &input.outpoint.index.to_string();
                let val: bool = match self
                    .transactions_unspent
                    .entry(input_outpoint_txid_idx.clone())
                {
                    Occupied(_) => true,
                    Vacant(_) => false,
                };

                if val {
                    self.transactions_unspent.remove(&input_outpoint_txid_idx);
                };
            }
            self.in_count += tx.value.in_count.value;

            // serialize outputs
            for (i, output) in tx.value.outputs.iter().enumerate() {
                let hash_val: HashMapVal = HashMapVal {
                    block_height,
                    output_val: output.out.value,
                    address: output.script.address.clone(),
                    //script_pubkey: utils::arr_to_hex(&output.out.script_pubkey)
                };
                self.transactions_unspent
                    .insert(txid_str.clone() + &i.to_string(), hash_val);
            }
            self.out_count += tx.value.out_count.value;
        }
        self.tx_count += block.tx_count.value;
    }

    fn on_complete(&mut self, block_height: usize) {
        self.end_height = block_height;
        for (key, value) in self.transactions_unspent.iter() {
            let txid = &key[0..64];
            let index = &key[64..];
            // let  = key.len();
            // let mut mut_key = key.clone();
            // let index: String = mut_key.pop().unwrap().to_string();
            // let docs = vec![
            //     doc!{"txid": txid, "indexOut": index, "height" : value.block_height, "value": value.output_val, "address": value.address}
            // ];
            self.collection.as_ref().unwrap().insert_one(doc!{"txid": txid, "indexOut": index, "height" : value.block_height as i64, "value": value.output_val, "address": value.address.clone()}, None);
        }

    

        info!(target: "callback", "Done.\nDumped all {} blocks:\n\
                                   \t-> transactions: {:9}\n\
                                   \t-> inputs:       {:9}\n\
                                   \t-> outputs:      {:9}",
             self.end_height + 1, self.tx_count, self.in_count, self.out_count);
    }
}
