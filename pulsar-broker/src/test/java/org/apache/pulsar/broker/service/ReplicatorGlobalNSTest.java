/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service;

import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * The tests in this class should be denied in a production pulsar cluster. they are very dangerous, which leads to
 * a lot of topic deletion and makes namespace policies being incorrect.
 */
@CustomLog
@Test(groups = "broker-replication")
public class ReplicatorGlobalNSTest extends ReplicatorTestBase {

    protected String methodName;
    @DataProvider(name = "loadManagerClassName")
    public static Object[][] loadManagerClassName() {
        return new Object[][]{
                {ModularLoadManagerImpl.class.getName()},
                {ExtensibleLoadManagerImpl.class.getName()}
        };
    }

    @Factory(dataProvider = "loadManagerClassName")
    public ReplicatorGlobalNSTest(String loadManagerClassName) {
        this.loadManagerClassName = loadManagerClassName;
    }

    @BeforeMethod
    public void beforeMethod(Method m) {
        methodName = m.getName();
    }

    @Override
    @BeforeClass(timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    /**
     * If local cluster is removed from the global namespace then all topics under that namespace should be deleted from
     * the cluster.
     *
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    @Test(priority = Integer.MAX_VALUE)
    public void testRemoveLocalClusterOnGlobalNamespace() throws Exception {
        log.info("--- Starting ReplicatorTest::testRemoveLocalClusterOnGlobalNamespace ---");

        final String namespace = "pulsar/removeClusterTest";
        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"), false);

        final String topicName = "persistent://" + namespace + "/topic";

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        ProducerImpl<byte[]> producer1 = (ProducerImpl<byte[]>) client1.newProducer().topic(topicName)
                .enableBatching(false).messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) client1.newConsumer().topic(topicName)
                .subscriptionName("sub1").subscribe();
        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) client2.newConsumer().topic(topicName)
                .subscriptionName("sub1").subscribe();

        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r2", "r3"), false);

        Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            Assert.assertFalse(pulsar1.getBrokerService().getTopics().containsKey(topicName));
            Assert.assertFalse(producer1.isConnected());
            Assert.assertFalse(consumer1.isConnected());
            Assert.assertTrue(consumer2.isConnected());
        });
    }

    /**
     * Clearing the replication clusters of a namespace disables the replication: the topics are not deleted on any
     * cluster and keep being served locally, but the data is not replicated between the clusters anymore.
     * See issue #24640.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testDisableNamespaceReplicationWithEmptyClusters() throws Exception {
        final String namespace = BrokerTestUtil.newUniqueName("pulsar/disable-repl");
        admin1.namespaces().createNamespace(namespace, Sets.newHashSet("r1", "r2", "r3"));

        final String topicName = "persistent://" + namespace + "/topic";
        final TopicName topic = TopicName.get(topicName);

        @Cleanup
        PulsarClient client1 = PulsarClient.builder().serviceUrl(url1.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        @Cleanup
        PulsarClient client2 = PulsarClient.builder().serviceUrl(url2.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();

        ProducerImpl<byte[]> producer1 = (ProducerImpl<byte[]>) client1.newProducer().topic(topicName)
                .enableBatching(false).messageRoutingMode(MessageRoutingMode.SinglePartition).create();
        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) client1.newConsumer().topic(topicName)
                .subscriptionName("sub1").subscribe();
        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) client2.newConsumer().topic(topicName)
                .subscriptionName("sub1").subscribe();

        producer1.send("message-1".getBytes());

        // The message is replicated to cluster r2.
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Topic replTopic1 = pulsar1.getBrokerService().getTopicReference(topic.toString()).orElseThrow();
            Topic replTopic2 = pulsar2.getBrokerService().getTopicReference(topic.toString()).orElseThrow();
            Assert.assertEquals(replTopic1.getReplicators().size(), 2);
            Assert.assertEquals(replTopic2.getReplicators().size(), 2);
        });

        // Disable the namespace-level replication with an empty cluster list.
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(), false);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(admin1.namespaces().getPolicies(namespace).replication_clusters.size(), 0));
        Policies policies = admin1.namespaces().getPolicies(namespace);
        Assert.assertTrue(policies.replication_clusters.isEmpty());
        // The clusters that served the namespace keep access to it.
        Assert.assertEquals(policies.allowed_clusters, Sets.newHashSet("r1", "r2", "r3"));

        // The replicators are stopped, so no more data is replicated between the clusters.
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Topic replTopic1 = pulsar1.getBrokerService().getTopicReference(topic.toString()).orElseThrow();
            Topic replTopic2 = pulsar2.getBrokerService().getTopicReference(topic.toString()).orElseThrow();
            Assert.assertTrue(replTopic1.getReplicators().isEmpty());
            Assert.assertTrue(replTopic2.getReplicators().isEmpty());
        });

        // Unlike removing a single cluster from the replication list, disabling the replication does not delete the
        // topics: the local topics keep being served on every cluster.
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertTrue(producer1.isConnected());
            Assert.assertTrue(consumer1.isConnected());
            Assert.assertTrue(consumer2.isConnected());
        });
        producer1.send("message-2".getBytes());
        Assert.assertTrue(pulsar2.getBrokerService().getTopics().containsKey(topicName));

        // Re-enabling the replication restores the replicators.
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"), false);
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            Topic replTopic1 = pulsar1.getBrokerService().getTopicReference(topic.toString()).orElseThrow();
            Assert.assertEquals(replTopic1.getReplicators().size(), 2);
        });

        producer1.close();
        consumer1.close();
        consumer2.close();
        client1.close();
        client2.close();
    }

    /**
     * This is not a formal operation and can cause serious problems if call it in a production environment.
     */
    @Test(priority = Integer.MAX_VALUE - 1)
    public void testConfigChange() throws Exception {
        log.info("--- Starting ReplicatorTest::testConfigChange ---");
        // This test is to verify that the config change on global namespace is successfully applied in broker during
        // runtime.
        // Run a set of producer tasks to create the topics
        List<Future<Void>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final TopicName dest = TopicName.get(BrokerTestUtil.newUniqueName("persistent://pulsar/ns/topic-" + i));

            results.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                    @Cleanup
                    MessageProducer producer = new MessageProducer(url1, dest);
                    log.info().attr("url1", url1).log("--- Starting producer ---");

                    @Cleanup
                    MessageConsumer consumer = new MessageConsumer(url1, dest);
                    log.info().attr("url1", url1).log("--- Starting Consumer ---");

                    producer.produce(2);
                    consumer.receive(2);
                    return null;
                }
            }));
        }

        for (Future<Void> result : results) {
            try {
                result.get();
            } catch (Exception e) {
                log.error().exception(e).log("exception in getting future result ");
                fail(String.format("replication test failed with %s exception", e.getMessage()));
            }
        }

        Thread.sleep(1000L);
        // Make sure that the internal replicators map contains remote cluster info
        final var replicationClients1 = ns1.getReplicationClients();
        final var replicationClients2 = ns2.getReplicationClients();
        final var replicationClients3 = ns3.getReplicationClients();

        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 1: Update the global namespace replication configuration to only contains the local cluster itself
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1"), false);

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 2: Update the configuration back
        admin1.namespaces().setNamespaceReplicationClusters("pulsar/ns", Sets.newHashSet("r1", "r2", "r3"), false);

        // Wait for config changes to be updated.
        Thread.sleep(1000L);

        // Make sure that the internal replicators map still contains remote cluster info
        Assert.assertNotNull(replicationClients1.get("r2"));
        Assert.assertNotNull(replicationClients1.get("r3"));
        Assert.assertNotNull(replicationClients2.get("r1"));
        Assert.assertNotNull(replicationClients2.get("r3"));
        Assert.assertNotNull(replicationClients3.get("r1"));
        Assert.assertNotNull(replicationClients3.get("r2"));

        // Case 3: TODO: Once automatic cleanup is implemented, add tests case to verify auto removal of clusters
    }
}
