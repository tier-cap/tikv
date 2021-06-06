// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use raftstore::store::msg::*;
use std::time::Duration;
use test_raftstore::*;
use tikv_util::config::*;
use tikv_util::sys::disk;

fn fail_split_region(cluster: &mut Cluster<ServerCluster>) {
    let key_str = format!("{:09}", 10 * 10000);
    let split_key = key_str.into_bytes();
    let region = cluster.get_region(&split_key);
    let split_count_1 = cluster.pd_client.get_split_count();

    // let check = Box::new(move |write_resp: WriteResponse| {
    //     let mut resp = write_resp.response;
    //     if resp.get_header().has_error() {
    //         let error = resp.get_header().get_error();
    //         if error.has_epoch_not_match()
    //             || error.has_not_leader()
    //             || error.has_stale_command()
    //             || error
    //                 .get_message()
    //                 .contains("peer has not applied to current term")
    //         {
    //             warn!("fail to split: {:?}, ignore.", error);
    //             return;
    //         }
    //         panic!("failed to split: {:?}", resp);
    //     }
    //     let admin_resp = resp.mut_admin_response();
    //     let split_resp = admin_resp.mut_splits();
    //     let regions = split_resp.get_regions();
    //     assert_eq!(regions.len(), 2);
    //     assert_eq!(regions[0].get_end_key(), split_key.as_slice());
    //     assert_eq!(regions[0].get_end_key(), regions[1].get_start_key());
    // });
    cluster.split_region(&region, &split_key, Callback::None);
    let split_count_2 = cluster.pd_client.get_split_count();
    println!("total region count after split is {}", split_count_2);
    assert!(split_count_1 == split_count_2);
}

#[test]
fn test_disk_full() {
    let reserve = 1024 * 1024 * 100;
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.storage.reserve_space = ReadableSize(reserve);
    cluster.cfg.raft_store.pd_store_heartbeat_tick_interval =
        ReadableDuration(Duration::from_secs(3000)); //disable disk status update influence.
    cluster.run();
    let must_key = String::from("100").into_bytes();
    let must_value = String::from("100").into_bytes();
    {
        // test cluster working
        cluster.must_put(&must_key, &must_value);
        cluster.must_get(&must_key).unwrap();
    }

    let disk_full_t = "disk_full_t";
    fail::cfg(disk_full_t, "return").unwrap();
    let mut flag = disk::is_disk_full();
    println!("after fail set, disk full is {:?}", flag);
    fail::remove(disk_full_t);
    flag = disk::is_disk_full();
    println!("after fail remove, disk full is {:?}", flag);

    // {
    //     //allow transfer leader
    //     cluster.must_transfer_leader(1, new_peer(1, 1));
    // }
    // {
    //     // not allow business write.
    //     let key_2 = String::from("200").into_bytes();
    //     let value_2 = String::from("200").into_bytes();
    //     let rx = cluster.async_put(&key_2, &value_2).unwrap();
    //     match rx.recv_timeout(Duration::from_secs(10)) {
    //         Ok(resp) => {
    //             //leader disk full
    //             assert!(resp.get_header().has_error());
    //         }
    //         //follower disk full
    //         Err(_) => {}
    //     }
    // }

    // {
    //     //split region not allowed
    //     fail_split_region(&mut cluster);
    // }

    // {
    //     //allow readonly
    //     cluster.must_get(&must_key).unwrap();

    //     //allow config change
    //     let rx = cluster.async_remove_peer(1, new_peer(1, 1)).unwrap();
    //     match rx.recv_timeout(Duration::from_secs(10)) {
    //         Ok(resp) => {
    //             assert!(!resp.get_header().has_error());
    //         }
    //         Err(e) => {
    //             panic!("remove peer error when disk full: {:?}", e);
    //         }
    //     }
    // }

    //fail::remove(disk_full_t);
    println!("disk full test over");
}
