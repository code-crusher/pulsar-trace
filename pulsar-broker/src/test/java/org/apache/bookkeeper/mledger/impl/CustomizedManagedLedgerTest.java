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
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class CustomizedManagedLedgerTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @SuppressWarnings("deprecation")
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setManagedLedgerStorageClassName(
            CustomizedManagedLedgerStorageForTest.class.getName());
        conf.setManagedLedgerCacheSizeMB(10);
        conf.setManagedLedgerCacheEvictionFrequency(1000);
        conf.setManagedLedgerCacheEvictionTimeThresholdMillis(1000);
    }

    @Test
    public void testNoMemoryLeakWhenExpireMessages() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";

        // Create topic with "CustomizedManagedLedger". The per-subscription message expiry, which the topic-level
        // expiry without a shared position and the admin API trigger, reads entries on the customized cursor and
        // must release them.
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(10);
        int messageTTLInSecond = 1;
        ManagedLedger ml = pulsar.getDefaultManagedLedgerFactory()
            .open(TopicName.get(topic).getPersistenceNamingEncoding(), config);
        CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger customizedManagedLedger =
                (CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger) ml;
        assertTrue(ml instanceof CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subscription).subscribe();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        for (int i = 0; i < 50; i++) {
            producer.send("msg-" + i);
        }

        // Trigger a message expiring for this subscription, which reads the oldest entry to check whether it
        // expired. The topic-level expiry in the next test shares the position to expire up to between the
        // subscriptions and does not read the oldest entry.
        ManagedCursor managedCursor = persistentTopic.getSubscription(subscription).getCursor();
        assertTrue(managedCursor instanceof CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator);
        CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator cursorDecorator =
                (CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator) managedCursor;
        Thread.sleep(TimeUnit.SECONDS.toMillis(messageTTLInSecond) * 3);
        Awaitility.await().untilAsserted(() -> {
            persistentTopic.getSubscription(subscription).expireMessagesAsync(messageTTLInSecond);
            assertEquals(cursorDecorator.getNumberOfEntriesInBacklog(true), 0);
        });

        // Verify: no memory leak.
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            assertFalse(cursorDecorator.entryReleasedStatusMap.isEmpty());
            for (Boolean released : cursorDecorator.entryReleasedStatusMap.values()) {
                assertTrue(released);
            }
        });

        // cleanup.
        producer.close();
        consumer.close();
        admin.topics().delete(topic);
    }

    /**
     * A managed ledger that does not extend {@code ManagedLedgerImpl} still exposes its cursors through a
     * {@link ManagedCursorContainer}, so the message expiry must share the position to expire up to between the
     * subscriptions, instead of falling back to searching a position per subscription.
     */
    @Test
    public void testExpireMessagesWithSharedPositionForCustomizedManagedLedger() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription1 = "s1";
        final String subscription2 = "s2";

        // Create topic with "CustomizedManagedLedger", which is not an instance of ManagedLedgerImpl.
        ManagedLedger ml = pulsar.getDefaultManagedLedgerFactory()
                .open(TopicName.get(topic).getPersistenceNamingEncoding(), new ManagedLedgerConfig());
        assertTrue(ml instanceof CustomizedManagedLedgerStorageForTest.CustomizedManagedLedger);
        assertFalse(ml instanceof ManagedLedgerImpl);
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        Consumer<String> consumer1 = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subscription1).subscribe();
        Consumer<String> consumer2 = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName(subscription2).subscribe();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        int messageTTLInSecond = 1;
        admin.topicPolicies().setMessageTTL(topic, messageTTLInSecond);
        for (int i = 0; i < 20; i++) {
            producer.send("msg-" + i);
        }

        CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator cursorDecorator1 =
                (CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator)
                        persistentTopic.getSubscription(subscription1).getCursor();
        CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator cursorDecorator2 =
                (CustomizedManagedLedgerStorageForTest.ManagedCursorDecorator)
                        persistentTopic.getSubscription(subscription2).getCursor();
        Thread.sleep(TimeUnit.SECONDS.toMillis(messageTTLInSecond) * 3);

        // The cursors are exposed through a ManagedCursorContainer, so the position to expire up to is searched
        // once for the whole topic, on the cursor of the container, and never on the subscriptions' cursors.
        Awaitility.await().untilAsserted(() -> {
            persistentTopic.checkMessageExpiry();
            assertEquals(cursorDecorator1.getNumberOfEntriesInBacklog(true), 0);
            assertEquals(cursorDecorator2.getNumberOfEntriesInBacklog(true), 0);
        });
        assertEquals(cursorDecorator1.findNewestMatchingCallCount.get(), 0);
        assertEquals(cursorDecorator2.findNewestMatchingCallCount.get(), 0);

        // cleanup.
        producer.close();
        consumer1.close();
        consumer2.close();
        admin.topics().delete(topic);
    }
}
