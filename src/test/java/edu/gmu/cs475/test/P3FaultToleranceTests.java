package edu.gmu.cs475.test;


import edu.gmu.cs475.internal.TestingClient;
import org.apache.logging.log4j.core.config.plugins.util.ResolverUtil;
import org.junit.Assert;
import org.junit.Test;
import org.netcrusher.NetCrusher;

import javax.validation.constraints.AssertTrue;
import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class P3FaultToleranceTests extends Base475Test {


    @Test(expected = IOException.class)
    public void node_disconnects_when_reading_throws_exception() throws Exception{
        TestingClient firstClient = newClient("Leader");
        try {
            blockUntilLeader(firstClient);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        TestingClient secondClient = newClient("follower");
        blockUntilMemberJoins(secondClient);
        firstClient.setValue("hello","trash");
        // disconnect second client from zookeeper and then call get
        secondClient.suspendAccessToZK();
        blockUntilMemberLeaves(secondClient);
        secondClient.getValue("hello");

    }

    @Test(expected = IOException.class)
    public void node_disconnects_when_writing_throws_exception() throws Exception{
        TestingClient firstClient = newClient("Leader");
        try {
            blockUntilLeader(firstClient);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        TestingClient secondClient = newClient("follower");
        blockUntilMemberJoins(secondClient);
        secondClient.setValue("hello","trash");
        // disconnect second client from zookeeper and then call get
        secondClient.suspendAccessToZK();
        blockUntilMemberLeaves(secondClient);
        secondClient.setValue("hello","New Trash");

    }

    @Test(timeout = 40000)
    public void reader_connected_leader_disconnected() throws  Exception{
        TestingClient leader = newClient("Leader");
        blockUntilLeader(leader);
        String key = getNewKey();
        String val = getNewValue();
        leader.setValue(key,val);
        TestingClient follower = newClient("Follower1");
        follower.getValue(key);
        TestingClient follower2 = newClient("Follower2");
        leader.suspendAccessToSelf();
        leader.suspendAccessToZK();
        blockUntilMemberLeaves(leader);
        try {
            Assert.assertEquals(val,follower2.getValue(key));
        }catch (Exception e){ }

    }

    // client will become leader if there are no candidates will still return
    @Test(timeout = 40000)
    public void reader_connected_leader_disconnected2() throws  Exception{
        TestingClient leader = newClient("Leader");
        blockUntilLeader(leader);
        String key = getNewKey();
        String val = getNewValue();
        leader.setValue(key,val);
        TestingClient follower2 = newClient("Follower2");
        leader.suspendAccessToSelf();
        leader.suspendAccessToZK();
        blockUntilMemberLeaves(leader);
        try {
            Assert.assertEquals(null,follower2.getValue(key));
        }catch (Exception e){ }

    }

    // can keep cache after reconnection
    @Test(timeout = 40000)
    public void only_cient_connected_keep_cache() throws  Exception{
        TestingClient leader = newClient("Leader");
        blockUntilLeader(leader);
        String key = getNewKey();
        String val = getNewValue();
        leader.setValue(key,val);
        leader.suspendAccessToSelf();
        leader.suspendAccessToZK();
        blockUntilMemberLeaves(leader);
        leader.resumeAccessToSelf();
        leader.resumeAccessToZK();
        blockUntilMemberJoins(leader);
        try {
            Assert.assertEquals(val,leader.getValue(key));
        }catch (Exception e){ }

    }

    // more clients connected do not keep cache
    @Test(timeout = 40000)
    public void not_alone_cient_connected_flush_cache() throws  Exception{
        TestingClient leader = newClient("Leader");
        TestingClient follower = newClient("extra");
        blockUntilLeader(leader);
        String key = getNewKey();
        String val = getNewValue();
        leader.setValue(key,val);
        leader.suspendAccessToSelf();
        leader.suspendAccessToZK();
        blockUntilMemberLeaves(leader);
        leader.resumeAccessToSelf();
        leader.resumeAccessToZK();
        blockUntilMemberJoins(leader);
        try {
            Assert.assertEquals(null,leader.getValue(key));
            Assert.assertEquals(null,follower.getValue(key));
        }catch (Exception e){ }

    }

    @Test(timeout = 40000)
    public void new_leader_all_other_caches_should_be_flushed() throws  Exception{
        TestingClient leader = newClient("Leader");
        blockUntilLeader(leader);
        TestingClient follower = newClient("extra");
        TestingClient follower2 = newClient("extra2");
        blockUntilMemberJoins(follower);
        blockUntilMemberJoins(follower2);
        String a1 = getNewKey();
        String a2 = getNewKey();
        String val1 = "exlusive to follower 1 ";
        String val2 = "exlusive to follower 2 ";
        leader.setValue(a1,val1);
        leader.setValue(a2,val2);
        follower.getValue(a1); // value cached in follower
        follower2.getValue(a2);
        leader.suspendAccessToSelf();
        leader.suspendAccessToZK();
        blockUntilMemberLeaves(leader);
        if(blockUntilLeader(follower)){ // follower1 became leader follower 2 should not have a2
            Assert.assertEquals(null, follower2.getValue(a2));
        }else{ // follower 2 becomes the leader follower 1 should not have a1
            Assert.assertEquals(null, follower.getValue(a1));
        }

    }


    // test if you become the leader you get to keep your cache after calling get
    @Test(timeout = 40000)
    public void leader_keeps_his_cache() throws  Exception{
        TestingClient leader = newClient("Leader");
        blockUntilLeader(leader);
        TestingClient follower = newClient("extra");
        blockUntilMemberJoins(follower);
        String a1 = getNewKey();
        String val1 = "exclusive to follower 1 ";
        leader.setValue(a1,val1);
        follower.getValue(a1); // value cached in follower
        leader.suspendAccessToZK();
        blockUntilMemberLeaves(leader);
        blockUntilLeader(follower);// follower should still have its values cached
        Assert.assertTrue(follower.getValue(a1).equals(val1));
    }

    // if client disconnects from zk when we are invalidating caches we continue
    @Test(timeout = 40000)
    public void invalidate_client_disconnects() throws  Exception{
        TestingClient leader = newClient("Leader");
        blockUntilLeader(leader);
        TestingClient follower = newClient("extra");
        TestingClient follower2 = newClient("extra");
        blockUntilMemberJoins(follower);
        blockUntilMemberJoins(follower2);
        String a1 = getNewKey();
        String val1 = "exclusive to follower 1 ";
        leader.setValue(a1,val1);
        follower.getValue(a1); // value cached in follower
        follower2.getValue(a1); // value cached in follower
        follower.suspendAccessToZK();
        blockUntilMemberLeaves(follower);
        follower2.setValue(a1,"newVal");
        Assert.assertTrue(follower2.getValue(a1).equals("newVal"));
    }

    // write blocks if client does not acknowledge
    // if client disconnects from zk when we are invalidating caches we continue
    @Test(timeout = 40000)
    public void invalidate_blocks_if_not_connect_to_client() throws  Exception{
        TestingClient leader = newClient("Leader");
        blockUntilLeader(leader);
        TestingClient follower = newClient("extra");
        TestingClient follower2 = newClient("extra");
        blockUntilMemberJoins(follower);
        blockUntilMemberJoins(follower2);
        String a1 = getNewKey();
        String val1 = "exclusive to follower 1 ";
        leader.setValue(a1,val1);
        follower.getValue(a1); // value cached in follower
        follower2.getValue(a1); // value cached in follower
        follower.suspendAccessToSelf();
        follower2.setValue(a1,"newVal");
        Assert.assertTrue(follower2.getValue(a1).equals("newVal"));
    }

}
