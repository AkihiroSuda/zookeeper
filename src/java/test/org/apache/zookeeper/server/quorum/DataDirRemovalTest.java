/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;


import org.apache.zookeeper.server.UnrecoverableException;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ReconfigTest;
import org.apache.zookeeper.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

public class DataDirRemovalTest extends QuorumPeerTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(DataDirRemovalTest.class);
    final int SERVER_COUNT = 2; // we are not interested in odd-node quorum
    final int quorumPorts[] = new int[SERVER_COUNT];
    final int electionPorts[] = new int[SERVER_COUNT];
    final int clientPorts[] = new int[SERVER_COUNT];
    final String servers[] = new String[SERVER_COUNT];
    final String quorumCfgSections[] = new String[SERVER_COUNT];
    final MainThread mt[] = new MainThread[SERVER_COUNT];
    final ZooKeeper zk[] = new ZooKeeper[SERVER_COUNT];

    @Before
    public void setUp() throws Exception {
        ClientBase.setupTestEnv();
        QuorumPeerConfig.setStandaloneEnabled(false);
        for (int i = 0; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            quorumPorts[i] = PortAssignment.unique();
            electionPorts[i] = PortAssignment.unique();
            servers[i] = String.format("server.%d=localhost:%d:%d:%s;localhost:%d",
                    i, quorumPorts[i], electionPorts[i], (i == 0 ? "participant" : "observer"),
                    clientPorts[i]);
        }
        quorumCfgSections[0] = servers[0] + "\n";
        quorumCfgSections[1] = servers[0] + "\n" + servers[1] + "\n";
    }

    private void startZk(int i) throws IOException {
        mt[i] = new MainThread(i, clientPorts[i], quorumCfgSections[i], false);
        zk[i] = new ZooKeeper("127.0.0.1:" + clientPorts[i],
                ClientBase.CONNECTION_TIMEOUT, this);
        mt[i].start();
    }

    private void waitForZk(int i) {
        Assert.assertTrue("waiting for server " + i + " being up",
                ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[i],
                        CONNECTION_TIMEOUT));
    }

    private void stopZk(int i) throws InterruptedException {
        zk[i].close();
        mt[i].shutdown();
    }

    private void waitForEpochGrowth(int trials, long sleep) throws IOException {
        boolean grown = true;
        for ( int i = 0; i < trials; i++) {
            try {
                Thread.sleep(sleep);
            }catch(InterruptedException ie) {
                LOG.warn("caught InterruptedException", ie);
            }
            for (int j = 0; j < SERVER_COUNT; j++) {
                long currentEpoch = mt[j].main.quorumPeer.getCurrentEpoch();
                long acceptedEpoch = mt[j].main.quorumPeer.getAcceptedEpoch();
                grown = (currentEpoch == 2) && (acceptedEpoch == 2);
            }
            if ( grown ) {
                return;
            }
        }
        if ( ! grown ) {
            throw new AssertionError("epochs did not grow in " + sleep + " milliseconds" +
                    " (tried " + trials + " times)" );
        }
    }

    /**
     * This test asserts that zk0 should raise UnrecoverableException
     * ("Leaders epoch, 1 is less than accepted epoch, 2")
     * when zk1 unexpectedly rebooted with an empty data dir.
     *
     * {@link https://issues.apache.org/jira/browse/ZOOKEEPER-2162}
     */
    @Test
    public void testDataDirRemoval() throws Exception {
        LOG.info("Starting zk0 and zk1 with an initial ensemble [zk0]");
        for (int i = 0; i < SERVER_COUNT; i++) {
            startZk(i);
        }
        for (int i = 0; i < SERVER_COUNT; i++) {
            waitForZk(i);
        }
        for (int i = 0; i < SERVER_COUNT; i++) {
            Assert.assertEquals(1, mt[i].main.quorumPeer.getCurrentEpoch());
            Assert.assertEquals(1, mt[i].main.quorumPeer.getAcceptedEpoch());
        }

        LOG.info("Invoking reconfig [zk0]->[zk1]");
        ReconfigTest.reconfig(zk[0], null, null,
                Arrays.asList(servers[1].replace("observer", "participant")), -1);

        LOG.info("Waiting for epoch growth (1->2)");
        waitForEpochGrowth(3, 10 * 1000);

        LOG.info("Stopping zk1");
        stopZk(1);

        File toBeDeleted = new File(mt[1].getTmpDir(), "data/version-2");
        LOG.info("Deleting data dir for zk1 ({})", toBeDeleted);
        Assert.assertTrue(toBeDeleted.exists());
        TestUtils.deleteFileRecursively(toBeDeleted);

        LOG.info("Installing UEH for zk0." );
        final CountDownLatch uehLatch = new CountDownLatch(1);
        mt[0].main.quorumPeer.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.warn("Exception occured from thread {}", t.getName(), e);
                if (e instanceof UnrecoverableException) {
                    LOG.info("UnrecoverableException is expected until ZOOKEEPER-2162 gets fixed someday.");
                    uehLatch.countDown();
                }
            }});

        LOG.info("Starting zk1");
        startZk(1);
        // After starting zk1, UnrecoverableException should be raised from zk0
        boolean caughtUnrecoverableException = uehLatch.await(60, TimeUnit.SECONDS);
        if (caughtUnrecoverableException) {
            return;
        }
        LOG.warn("Did not catch UnrecoverableException from zk0. ZOOKEEPER-2162 got fixed?");
        waitForZk(1);

        LOG.info("Invoking normal ops so as to check whether ZOOKEEPER-2162 got reproduced");
        ReconfigTest.testNormalOperation(zk[0], zk[1]);
        LOG.info("Did not hit ZOOKEEPER-2162");

        for (int i = 0; i < SERVER_COUNT; i++) {
            LOG.info("Stopping zk{}", i);
            stopZk(i);
        }
    }

}