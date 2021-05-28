// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_cmdpb::*;
use raftstore::store::msg::*;
use std::time::Duration;
use std::time::*;
use test_raftstore::*;
use tikv_util::config::*;
/*
Disk full test will have 3 scenarios.
[leader full]
1. not allowed: business wirte, region merge/split,etc.
2. allowed: read, config change(add/remove peer), transfer leader etc.

[minority full]
1. not allowed: log entry append
2. allowed: config change,transfer leader,etc.

[majority full]
1. equal to minority full, no additional design.
*/
const DISK_FULL_LEADER: &str = "disk_full_leader";
const DISK_FULL_FOLLOWER_1: &str = "disk_full_follower_1";

fn prepare_test_data(cluster: &mut Cluster<ServerCluster>) {
    let left_key = String::from("5555").into_bytes();
    let left_value = String::from("55550000").into_bytes();
    let split_key = String::from("6666").into_bytes();
    let region = cluster.get_region(b"");
    cluster.must_split(&region, &split_key);
    cluster.must_put(&left_key, &left_value);
    cluster.must_get(&left_key).unwrap();
    let right_key = String::from("7777").into_bytes();
    let right_value = String::from("77770000").into_bytes();
    cluster.must_put(&right_key, &right_value);
    cluster.must_get(&right_key);
}

fn fail_leader_full(cluster: &mut Cluster<ServerCluster>) {
    fail::cfg(DISK_FULL_LEADER, "return").unwrap();

    let leader = cluster.leader_of_region(1).unwrap();

    {
        let raft_stat = cluster.raft_local_state(1, leader.get_store_id());
        let index_1 = raft_stat.get_last_index();

        let key = String::from("8000").into_bytes();
        let value = String::from("8000").into_bytes();
        let rx = cluster.async_put(&key, &value).unwrap();
        is_error_response(&rx.recv_timeout(Duration::from_secs(10)).unwrap());

        let raft_stat = cluster.raft_local_state(1, leader.get_store_id());
        let index_2 = raft_stat.get_last_index();
        assert!(index_1 == index_2);
    }

    {
        let raft_stat = cluster.raft_local_state(1, leader.get_store_id());
        let index_1 = raft_stat.get_last_index();

        let split_key = String::from("0000").into_bytes();
        let region = cluster.get_region(&split_key);
        let split_count_before = cluster.pd_client.get_split_count();
        cluster.split_region(&region, &split_key, Callback::None);
        let split_count_after = cluster.pd_client.get_split_count();
        // println!(
        //     "fail split count: {} {}",
        //     split_count_before, split_count_after
        // );
        assert!(split_count_before == split_count_after);

        let raft_stat = cluster.raft_local_state(1, leader.get_store_id());
        let index_3 = raft_stat.get_last_index();
        assert!(index_1 == index_3);
    }

    {
        let raft_stat = cluster.raft_local_state(1, leader.get_store_id());
        let index_1 = raft_stat.get_last_index();

        let left_key = String::from("5555").into_bytes();
        let right_key = String::from("7777").into_bytes();
        let region1 = cluster.get_region(&left_key);
        let region2 = cluster.get_region(&right_key);
        assert!(region1.get_id() != region2.get_id());
        let resp = cluster.try_merge(region1.get_id(), region2.get_id());
        assert!(is_error_response(&resp));

        let raft_stat = cluster.raft_local_state(1, leader.get_store_id());
        let index_4 = raft_stat.get_last_index();
        assert!(index_1 == index_4);
    }

    fail::remove(DISK_FULL_LEADER);
}

fn success_leader_full(cluster: &mut Cluster<ServerCluster>) {
    let key = String::from("5555").into_bytes();
    fail::cfg(DISK_FULL_LEADER, "return").unwrap();
    cluster.must_get(&key).unwrap();

    let region = cluster.get_region(&key);
    let old_leader = cluster.leader_of_region(region.get_id()).unwrap();
    let peers = region.get_peers();

    let target = peers
        .into_iter()
        .find(|x| x.get_store_id() != old_leader.get_store_id())
        .unwrap();

    cluster.must_transfer_leader(region.get_id(), (*target).clone());
    let new_leader = cluster.leader_of_region(region.get_id()).unwrap();
    assert!(
        new_leader.get_store_id() != old_leader.get_store_id()
            && new_leader.get_id() != old_leader.get_id()
    );

    let pd_client = cluster.pd_client.clone();
    pd_client.must_remove_peer(region.get_id(), old_leader.clone());
    let region = cluster.get_region(&key);
    assert!(region.get_peers().len() == 2);

    let peer_3 = new_learner_peer(old_leader.get_store_id(), 20000);
    pd_client.must_add_peer(region.get_id(), peer_3); //can not!
    pd_client.must_add_peer(region.get_id(), new_peer(old_leader.get_store_id(), 20000));
    let region = cluster.get_region(&key);
    // for i in region.get_peers() {
    //     println!("peer info: {}-{}", i.get_store_id(), i.get_id());
    // }
    assert!(region.get_peers().len() == 3);
    //failed to propose confchange, error: Other("[components/raftstore/src/store/peer.rs:2469]: [region 1000] 1002 unsafe to perform conf change [peer { id: 2000 store_id: 1 }], before: voters: 1004 voters: 1002 voters: 1003, after: voters: 2000 voters: 1002 voters: 1004 voters: 1003, truncated index 5, promoted commit index 0")
    fail::remove(DISK_FULL_LEADER);
}

fn fail_follower_full(cluster: &mut Cluster<ServerCluster>) {
    let key = String::from("2222").into_bytes();
    let value = String::from("22220000").into_bytes();
    let region = cluster.get_region(&key);
    let target_peer = region
        .get_peers()
        .iter()
        .find(|x| x.get_store_id() == 3)
        .unwrap();
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    // if leader.get_store_id() != target_peer.get_store_id() {
    //     println!(
    //         "need transfer: leader store id {}, target peer store id {}",
    //         leader.get_store_id(),
    //         target_peer.get_store_id()
    //     );
    //     cluster.must_transfer_leader(region.get_id(), (*target_peer).clone());
    // } else {
    //     println!(
    //         "no need transfer: leader target peer store id equal, {}",
    //         target_peer.get_store_id()
    //     );
    // }
    // deal with config version older problems
    let region_id = region.get_id();
    if leader.get_store_id() != target_peer.get_store_id() {
        let new_leader = (*target_peer).clone();
        let timer = Instant::now();
        loop {
            cluster.reset_leader_of_region(region_id);
            let cur_leader = cluster.leader_of_region(region_id);
            if let Some(ref cur_leader) = cur_leader {
                if cur_leader.get_id() == new_leader.get_id()
                    && cur_leader.get_store_id() == new_leader.get_store_id()
                {
                    break;
                }
            }
            if timer.elapsed() > Duration::from_secs(5) {
                panic!(
                    "failed to transfer leader to [{}] {:?}, current leader: {:?}",
                    region_id, leader, cur_leader
                );
            }
            {
                let epoch = cluster.get_region_epoch(region_id);
                let transfer_leader = new_admin_request(
                    region_id,
                    &epoch,
                    new_transfer_leader_cmd(new_leader.clone()),
                );
                let resp = cluster
                    .call_command_on_leader(transfer_leader, Duration::from_secs(5))
                    .unwrap();
                if resp.get_admin_response().get_cmd_type() != AdminCmdType::TransferLeader {
                    continue;
                }
            }
        }
    }

    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let region = cluster.get_region(&key);
    let follower1 = region
        .get_peers()
        .iter()
        .find(|x| x.get_store_id() == 1)
        .unwrap();
    let follower2 = region
        .get_peers()
        .iter()
        .find(|x| x.get_store_id() == 2)
        .unwrap();

    fail::cfg(DISK_FULL_FOLLOWER_1, "return").unwrap();
    assert!(leader.get_store_id() == 3);
    assert!(follower1.get_store_id() == 1);
    assert!(follower2.get_store_id() == 2);
    {
        cluster.must_put(&key, &value);
        let leader_state = cluster.raft_local_state(region.get_id(), leader.get_store_id());
        let follower_state_1 = cluster.raft_local_state(region.get_id(), follower1.get_store_id());
        let follower_state_2 = cluster.raft_local_state(region.get_id(), follower2.get_store_id());
        assert!(leader_state.get_last_index() == follower_state_2.get_last_index());
        assert!(leader_state.get_last_index() != follower_state_1.get_last_index());
    }
    {
        //cluster.must_transfer_leader(region.get_id(), (*follower1).clone());
        let timer = Instant::now();
        loop {
            cluster.reset_leader_of_region(region.get_id());
            let cur_leader = cluster.leader_of_region(region.get_id());
            if let Some(ref cur_leader) = cur_leader {
                assert!(
                    !(cur_leader.get_id() == follower1.get_id()
                        && cur_leader.get_store_id() == follower1.get_store_id())
                );
            }
            if timer.elapsed() > Duration::from_secs(2) {
                //println!("transfer to followers with disk full will fail");
                break;
            }
            cluster.transfer_leader(region.get_id(), (*follower1).clone());
        }
    }
    fail::remove(DISK_FULL_FOLLOWER_1);
}

#[test]
fn test_disk_full() {
    let reserve = 1024 * 1024 * 100;
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.storage.reserve_space = ReadableSize(reserve);
    cluster.cfg.raft_store.pd_store_heartbeat_tick_interval =
        ReadableDuration(Duration::from_secs(3000)); //disable disk status update influence.
    cluster.run();
    cluster.pd_client.disable_default_operator();
    prepare_test_data(&mut cluster);
    success_leader_full(&mut cluster);
    fail_leader_full(&mut cluster);
    fail_follower_full(&mut cluster);
}
