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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.apache.pulsar.client.impl.GeoReplicationProducerImpl.MSG_PROP_REPL_SOURCE_POSITION;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.ForwardingConcurrentMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.qos.AsyncTokenBucket;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BacklogQuotaManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.compaction.CompactionServiceFactory;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class MessageDuplicationTest extends BrokerTestBase {

    private static final int BROKER_DEDUPLICATION_ENTRIES_INTERVAL = 10;
    private static final int BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS = 10;
    private static final String REPLICATOR_PREFIX = "foo";

    @Test
    public void testIsDuplicate() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        PersistentTopic persistentTopic = mock(PersistentTopic.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, persistentTopic, managedLedger);
        doReturn(true).when(messageDeduplication).isEnabled();

        String producerName1 = "producer1";
        ByteBuf byteBuf1 = getMessage(producerName1, 0);
        Topic.PublishContext publishContext1 = getPublishContext(producerName1, 0);

        String producerName2 = "producer2";
        ByteBuf byteBuf2 = getMessage(producerName2, 1);
        Topic.PublishContext publishContext2 = getPublishContext(producerName2, 1);

        MessageDeduplication.MessageDupStatus status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        byteBuf1.release();

        Long lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 0);

        status = messageDeduplication.isDuplicate(publishContext2, byteBuf2);
        byteBuf2.release();
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 1);
        publishContext1 = getPublishContext(producerName1, 1);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        byteBuf1.release();
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);

        byteBuf1 = getMessage(producerName1, 5);
        publishContext1 = getPublishContext(producerName1, 5);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        byteBuf1.release();
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        byteBuf1.release();
        // should expect unknown because highestSequencePersisted is empty
        assertEquals(status, MessageDeduplication.MessageDupStatus.Unknown);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        // update highest sequence persisted
        messageDeduplication.setHighestSequencedPersisted(producerName1, 0L);

        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        byteBuf1.release();
        // now that highestSequencedPersisted, message with seqId of zero can be classified as a dup
        assertEquals(status, MessageDeduplication.MessageDupStatus.Dup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        // update highest sequence persisted
        messageDeduplication.highestSequencedPushed.put(producerName1, 0L);
        messageDeduplication.setHighestSequencedPersisted(producerName1, 0L);
        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 1, 5);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        assertEquals(status, MessageDeduplication.MessageDupStatus.NotDup);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);

        publishContext1 = getPublishContext(producerName1, 4, 8);
        status = messageDeduplication.isDuplicate(publishContext1, byteBuf1);
        byteBuf1.release();
        assertEquals(status, MessageDeduplication.MessageDupStatus.Unknown);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
    }

    @Test
    public void testSnapshotKeepsTheLatestSequenceIdsOfUpToMaxProducers() {
        MessageDeduplication messageDeduplication = messageDeduplication(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        for (int i = 0; i < 15; i++) {
            messageDeduplication.setHighestSequencedPersisted("producer" + i, i);
        }
        MessageDeduplication.PersistedSequenceId persisted =
                messageDeduplication.highestSequencedPersisted.get("producer0");
        // updated in place
        for (int i = 0; i < 15; i++) {
            messageDeduplication.setHighestSequencedPersisted("producer" + i, 100 + i);
        }
        assertSame(messageDeduplication.highestSequencedPersisted.get("producer0"), persisted);
        Map<String, Long> snapshot = messageDeduplication.snapshotPersistedSequenceIds();
        assertEquals(snapshot.size(), BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        snapshot.forEach((producerName, sequenceId) ->
                assertEquals((long) sequenceId, 100 + Long.parseLong(producerName.substring("producer".length()))));

        for (int maxNumberOfProducers : new int[] {0, -1, -10}) {
            MessageDeduplication capped = messageDeduplication(maxNumberOfProducers);
            capped.setHighestSequencedPersisted("producer", 1);
            assertTrue(capped.snapshotPersistedSequenceIds().isEmpty());
        }
    }

    @Test
    public void testAPurgeRacingAnUpdateOrdersAfterIt() {
        // the purge runs between the update's lookup and its write, or after the update
        for (boolean purgeDuringUpdate : new boolean[] {true, false}) {
            PulsarService pulsarService = mock(PulsarService.class);
            ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
            serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
            serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
            serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
            serviceConfiguration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(1);
            doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
            MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                    pulsarService, mock(PersistentTopic.class), mock(ManagedLedger.class));
            doReturn(true).when(messageDeduplication).isEnabled();
            doReturn(mock(ManagedCursor.class)).when(messageDeduplication).getManagedCursor();

            // the producer has persisted up to 4 and publishes 5, then expires while 5 is being persisted
            String producerName = "producer";
            messageDeduplication.setHighestSequencedPersisted(producerName, 4);
            assertEquals(messageDeduplication.isDuplicate(getPublishContext(producerName, 5), null),
                    MessageDeduplication.MessageDupStatus.NotDup);
            messageDeduplication.producerRemoved(producerName);
            messageDeduplication.getInactiveProducers().put(producerName, System.currentTimeMillis() - 120_000);
            ConcurrentMap<String, MessageDeduplication.PersistedSequenceId> purgedOnLookup =
                    new ForwardingConcurrentMap<>() {
                        @Override
                        protected ConcurrentMap<String, MessageDeduplication.PersistedSequenceId> delegate() {
                            return messageDeduplication.highestSequencedPersisted;
                        }

                        @Override
                        public MessageDeduplication.PersistedSequenceId get(Object key) {
                            MessageDeduplication.PersistedSequenceId value = super.get(key);
                            messageDeduplication.purgeInactiveProducers();
                            return value;
                        }
                    };
            if (purgeDuringUpdate) {
                MessageDeduplication.setHighestSequencedPersisted(purgedOnLookup, producerName, 5);
            } else {
                messageDeduplication.setHighestSequencedPersisted(producerName, 5);
                messageDeduplication.purgeInactiveProducers();
            }
            assertFalse(messageDeduplication.highestSequencedPersisted.containsKey(producerName));
            assertFalse(messageDeduplication.highestSequencedPushed.containsKey(producerName));

            // a producer with the same name restarts at 0: a retry before 0 is persisted isn't a known duplicate
            assertEquals(messageDeduplication.isDuplicate(getPublishContext(producerName, 0), null),
                    MessageDeduplication.MessageDupStatus.NotDup);
            assertEquals(messageDeduplication.isDuplicate(getPublishContext(producerName, 0), null),
                    MessageDeduplication.MessageDupStatus.Unknown);
            messageDeduplication.setHighestSequencedPersisted(producerName, 0);
            assertEquals(messageDeduplication.isDuplicate(getPublishContext(producerName, 0), null),
                    MessageDeduplication.MessageDupStatus.Dup);
        }
    }

    @Test
    public void testRecordingARemovedProducerAddsANewEntry() {
        MessageDeduplication messageDeduplication = messageDeduplication(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        messageDeduplication.setHighestSequencedPersisted("producer", 1);
        MessageDeduplication.PersistedSequenceId persisted =
                messageDeduplication.highestSequencedPersisted.get("producer");
        // a purge of inactive producers removes the entry
        messageDeduplication.highestSequencedPersisted.remove("producer");
        messageDeduplication.setHighestSequencedPersisted("producer", 2);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("producer").value, 2L);
        assertEquals(persisted.value, 1L);
        assertEquals(messageDeduplication.snapshotPersistedSequenceIds(), Map.of("producer", 2L));
    }

    private static MessageDeduplication messageDeduplication(int maxNumberOfProducers) {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(maxNumberOfProducers);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        return new MessageDeduplication(pulsarService, mock(PersistentTopic.class), mock(ManagedLedger.class));
    }

    @Test
    public void testConcurrentDuplicateCheckForSameProducer() throws Exception {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, mock(PersistentTopic.class), mock(ManagedLedger.class));
        doReturn(true).when(messageDeduplication).isEnabled();

        int threads = 32;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ByteBuf message = getMessage("producer", 1);
        List<Future<MessageDeduplication.MessageDupStatus>> results = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                Topic.PublishContext context = getPublishContext("producer", 1);
                results.add(executor.submit(() -> {
                    start.await();
                    return messageDeduplication.isDuplicate(context, message);
                }));
            }
            start.countDown();

            int accepted = 0;
            int unknown = 0;
            for (Future<MessageDeduplication.MessageDupStatus> result : results) {
                switch (result.get()) {
                    case NotDup -> accepted++;
                    case Unknown -> unknown++;
                    default -> throw new AssertionError("A sequence cannot be known persisted before completion");
                }
            }
            assertEquals(accepted, 1);
            assertEquals(unknown, threads - 1);
            assertEquals(messageDeduplication.highestSequencedPushed.get("producer").longValue(), 1L);
        } finally {
            message.release();
            executor.shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInactiveProducerRemove() throws Exception {
        PulsarService pulsarService = mock(PulsarService.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);

        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(1);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, topic, managedLedger);
        doReturn(true).when(messageDeduplication).isEnabled();

        ManagedCursor managedCursor = mock(ManagedCursor.class);
        doReturn(managedCursor).when(messageDeduplication).getManagedCursor();

        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);

        Field field = MessageDeduplication.class.getDeclaredField("inactiveProducers");
        field.setAccessible(true);
        Map<String, Long> inactiveProducers = (ConcurrentHashMap<String, Long>) field.get(messageDeduplication);

        String producerName1 = "test1";
        when(publishContext.getHighestSequenceId()).thenReturn(2L);
        when(publishContext.getSequenceId()).thenReturn(1L);
        when(publishContext.getProducerName()).thenReturn(producerName1);
        messageDeduplication.isDuplicate(publishContext, null);

        String producerName2 = "test2";
        when(publishContext.getProducerName()).thenReturn(producerName2);
        messageDeduplication.isDuplicate(publishContext, null);

        String producerName3 = "test3";
        when(publishContext.getProducerName()).thenReturn(producerName3);
        messageDeduplication.isDuplicate(publishContext, null);

        // All 3 are added to the inactiveProducers list
        messageDeduplication.producerRemoved(producerName1);
        messageDeduplication.producerRemoved(producerName2);
        messageDeduplication.producerRemoved(producerName3);

        // Try first purgeInactive, all producer not inactive.
        messageDeduplication.purgeInactiveProducers();
        assertEquals(inactiveProducers.size(), 3);

        doReturn(false).when(messageDeduplication).isEnabled();
        inactiveProducers.put(producerName2, System.currentTimeMillis() - 80000);
        inactiveProducers.put(producerName3, System.currentTimeMillis() - 80000);
        messageDeduplication.purgeInactiveProducers();
        assertFalse(inactiveProducers.containsKey(producerName2));
        assertFalse(inactiveProducers.containsKey(producerName3));
        doReturn(true).when(messageDeduplication).isEnabled();
        // Modify the inactive time of produce2 and produce3
        // messageDeduplication.purgeInactiveProducers() will remove producer2 and producer3
        inactiveProducers.put(producerName2, System.currentTimeMillis() - 70000);
        inactiveProducers.put(producerName3, System.currentTimeMillis() - 70000);
        // Try second purgeInactive, produce2 and produce3 is inactive.
        messageDeduplication.purgeInactiveProducers();
        assertFalse(inactiveProducers.containsKey(producerName2));
        assertFalse(inactiveProducers.containsKey(producerName3));
        final var highestSequencedPushed = messageDeduplication.highestSequencedPushed;

        assertEquals((long) highestSequencedPushed.get(producerName1), 2L);
        assertFalse(highestSequencedPushed.containsKey(producerName2));
        assertFalse(highestSequencedPushed.containsKey(producerName3));
    }

    @Test
    public void testInactiveGeoReplicationProducerKeepsDedupState() throws Exception {
        PulsarService pulsarService = mock(PulsarService.class);
        PersistentTopic topic = mock(PersistentTopic.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);

        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setBrokerDeduplicationProducerInactivityTimeoutMinutes(1);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, topic, managedLedger);
        doReturn(true).when(messageDeduplication).isEnabled();

        ManagedCursor managedCursor = mock(ManagedCursor.class);
        doReturn(PositionFactory.create(0, 0)).when(managedCursor).getMarkDeletedPosition();
        doReturn(managedCursor).when(messageDeduplication).getManagedCursor();

        Map<String, Long> inactiveProducers = messageDeduplication.getInactiveProducers();

        String replicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        // Geo V2 dedup state tracks the last source position, so it must outlive producer inactivity cleanup.
        MessageDeduplication.ReplSourcePosition replSourcePosition =
                new MessageDeduplication.ReplSourcePosition(7L, 9L);
        messageDeduplication.highestReplPositionPushed.put(replicatorProducerName, replSourcePosition);
        messageDeduplication.highestReplPositionPersisted.put(replicatorProducerName, replSourcePosition);

        messageDeduplication.producerRemoved(replicatorProducerName);
        inactiveProducers.put(replicatorProducerName, System.currentTimeMillis() - 70000);

        messageDeduplication.purgeInactiveProducers();

        assertFalse(inactiveProducers.containsKey(replicatorProducerName));
        assertEquals(messageDeduplication.highestReplPositionPushed.get(replicatorProducerName), replSourcePosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(replicatorProducerName), replSourcePosition);
    }

    private MessageDeduplication newEnabledMessageDeduplication() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication messageDeduplication = spyWithClassAndConstructorArgs(MessageDeduplication.class,
                pulsarService, topic, mock(ManagedLedger.class));
        doReturn(true).when(messageDeduplication).isEnabled();
        return messageDeduplication;
    }

    private Topic.PublishContext getReplPublishContext(String producerName, AtomicReference<long[]> sourcePosition) {
        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);
        doReturn(producerName).when(publishContext).getProducerName();
        doReturn(true).when(publishContext).supportsReplDedupByLidAndEid();
        doAnswer(invocation -> MSG_PROP_REPL_SOURCE_POSITION.equals(invocation.getArgument(0))
                ? sourcePosition.get() : null).when(publishContext).getProperty(any());
        return publishContext;
    }

    @Test
    public void testGeoReplicationV2DeduplicationStateTransitions() {
        MessageDeduplication messageDeduplication = newEnabledMessageDeduplication();
        String replicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        AtomicReference<long[]> sourcePosition = new AtomicReference<>(new long[]{7L, 9L});
        Topic.PublishContext publishContext = getReplPublishContext(replicatorProducerName, sourcePosition);

        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Unknown);

        messageDeduplication.recordMessagePersisted(publishContext, PositionFactory.create(1L, 1L));
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Dup);

        sourcePosition.set(new long[]{7L, 8L});
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Dup);

        sourcePosition.set(new long[]{8L, 0L});
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Unknown);

        // Simulate an add failure: pushed state must roll back to the last persisted source position.
        messageDeduplication.resetHighestSequenceIdPushed();
        MessageDeduplication.ReplSourcePosition persistedPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 9L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(replicatorProducerName), persistedPosition);
        assertEquals(messageDeduplication.isDuplicateReplV2(publishContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
    }

    @Test
    public void testGeoReplicationV2RequiresValidSourcePosition() {
        MessageDeduplication messageDeduplication = newEnabledMessageDeduplication();
        String replicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);
        doReturn(replicatorProducerName).when(publishContext).getProducerName();
        doReturn(true).when(publishContext).supportsReplDedupByLidAndEid();
        Map<String, Object> properties = new HashMap<>();
        doAnswer(invocation -> {
            properties.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(publishContext).setProperty(any(), any());
        doAnswer(invocation -> properties.get(invocation.getArgument(0)))
                .when(publishContext).getProperty(any());

        for (String sourcePosition : new String[]{null, "invalid", "-1:9", "7:-1", "7:", ":9", "7:9:10",
                "9223372036854775808:9"}) {
            MessageMetadata metadata = new MessageMetadata()
                    .setProducerName("app-producer")
                    .setReplicatedFrom("c1")
                    .setSequenceId(3L)
                    .setPublishTime(System.currentTimeMillis());
            if (sourcePosition != null) {
                metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue(sourcePosition);
            }
            ByteBuf entry = serializeMetadataAndPayload(
                    Commands.ChecksumType.Crc32c, metadata, Unpooled.EMPTY_BUFFER);
            try {
                assertEquals(messageDeduplication.isDuplicate(publishContext, entry),
                        MessageDeduplication.MessageDupStatus.Unknown);
            } finally {
                entry.release();
            }
        }

        assertTrue(messageDeduplication.highestReplPositionPushed.isEmpty());
        assertTrue(messageDeduplication.highestReplPositionPersisted.isEmpty());
        assertTrue(messageDeduplication.highestSequencedPushed.isEmpty());
        assertTrue(messageDeduplication.highestSequencedPersisted.isEmpty());

        // Prove that this context carries a successfully parsed position through the V2 duplicate check.
        MessageMetadata validMetadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(3L)
                .setPublishTime(System.currentTimeMillis());
        validMetadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");
        ByteBuf validEntry = serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, validMetadata, Unpooled.EMPTY_BUFFER);
        try {
            assertEquals(messageDeduplication.isDuplicate(publishContext, validEntry),
                    MessageDeduplication.MessageDupStatus.NotDup);
            assertEquals(messageDeduplication.highestReplPositionPushed.get(replicatorProducerName),
                    new MessageDeduplication.ReplSourcePosition(7L, 9L));
        } finally {
            validEntry.release();
        }
    }

    @Test
    public void testGeoReplicationWatermarksAreIndependentByReplicator() {
        MessageDeduplication messageDeduplication = newEnabledMessageDeduplication();
        AtomicReference<long[]> sourcePosition = new AtomicReference<>(new long[]{7L, 9L});
        String firstReplicator = REPLICATOR_PREFIX + ".c1-->c3";
        String secondReplicator = REPLICATOR_PREFIX + ".c2-->c3";
        Topic.PublishContext firstContext = getReplPublishContext(firstReplicator, sourcePosition);
        Topic.PublishContext secondContext = getReplPublishContext(secondReplicator, sourcePosition);

        assertEquals(messageDeduplication.isDuplicateReplV2(firstContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
        assertEquals(messageDeduplication.isDuplicateReplV2(secondContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.NotDup);
        messageDeduplication.recordMessagePersisted(firstContext, PositionFactory.create(1L, 1L));
        messageDeduplication.recordMessagePersisted(secondContext, PositionFactory.create(1L, 2L));

        assertEquals(messageDeduplication.isDuplicateReplV2(firstContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Dup);
        assertEquals(messageDeduplication.isDuplicateReplV2(secondContext, Unpooled.EMPTY_BUFFER),
                MessageDeduplication.MessageDupStatus.Dup);
        assertEquals(messageDeduplication.highestReplPositionPersisted.size(), 2);
    }

    @Test(dataProvider = "snapshotProducerLimits")
    public void testSnapshotStoresCompleteReplicationWatermarkPairsWithinProducerLimit(
            int limit, int normalProducerCount, int replicatorCount) {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(1);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(limit);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();

        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        ManagedCursor managedCursor = mock(ManagedCursor.class);
        messageDeduplication.setManagedCursor(managedCursor);

        for (int i = 0; i < normalProducerCount; i++) {
            messageDeduplication.setHighestSequencedPersisted("normal-producer-" + i, 5L);
        }
        for (int i = 0; i < replicatorCount; i++) {
            String producerName = REPLICATOR_PREFIX + ".source-" + i + "-->target";
            Topic.PublishContext publishContext = getReplPublishContext(producerName,
                    new AtomicReference<>(new long[]{7L + i, 9L}));
            messageDeduplication.recordMessagePersistedRepl(publishContext, PositionFactory.create(1, 1));
        }

        Map<String, Long> snapshot = messageDeduplication.snapshotPersistedSequenceIds();
        int normalEntries = 0;
        for (int i = 0; i < normalProducerCount; i++) {
            String producerName = "normal-producer-" + i;
            if (snapshot.containsKey(producerName)) {
                assertEquals(snapshot.get(producerName), Long.valueOf(5L));
                normalEntries++;
            }
        }
        int replicationPairs = 0;
        for (int i = 0; i < replicatorCount; i++) {
            String producerName = REPLICATOR_PREFIX + ".source-" + i + "-->target";
            assertEquals(snapshot.containsKey(producerName + "_LID"), snapshot.containsKey(producerName + "_EID"));
            if (snapshot.containsKey(producerName + "_LID")) {
                assertEquals(snapshot.get(producerName + "_LID"), Long.valueOf(7L + i));
                assertEquals(snapshot.get(producerName + "_EID"), Long.valueOf(9L));
                replicationPairs++;
            }
        }
        assertEquals(normalEntries, Math.min(limit, normalProducerCount));
        assertEquals(replicationPairs, Math.min(limit, replicatorCount));
        assertEquals(snapshot.size(), normalEntries + 2 * replicationPairs);
    }

    @DataProvider
    public Object[][] snapshotProducerLimits() {
        return new Object[][]{{0, 2, 2}, {1, 1, 0}, {1, 0, 1}, {1, 1, 1}, {1, 2, 3}, {3, 4, 4}};
    }

    @Test
    public void testSnapshotRetainsLegacyKeysWithoutOverwritingReplicationWatermark() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(1);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(2);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        ManagedCursor managedCursor = mock(ManagedCursor.class);
        messageDeduplication.setManagedCursor(managedCursor);

        String replicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        String legacyProducerName = REPLICATOR_PREFIX + "-application_LID";
        Map<String, Long> oldSnapshot = Map.of(
                "normal-producer", 5L,
                legacyProducerName, 11L,
                replicatorProducerName + "_LID", 7L,
                replicatorProducerName + "_EID", 9L);
        messageDeduplication.recoverDeduplicationStateFromSnapshot(oldSnapshot);

        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);
        doReturn(replicatorProducerName).when(publishContext).getProducerName();
        doReturn(new long[]{8L, 0L}).when(publishContext).getProperty(MSG_PROP_REPL_SOURCE_POSITION);

        messageDeduplication.recordMessagePersistedRepl(publishContext, PositionFactory.create(1, 1));

        Map<String, Long> snapshot = messageDeduplication.snapshotPersistedSequenceIds();
        assertEquals(snapshot, Map.of(
                "normal-producer", 5L,
                legacyProducerName, 11L,
                replicatorProducerName + "_LID", 8L,
                replicatorProducerName + "_EID", 0L));
    }

    @Test
    public void testRecoverOldSnapshotWithNormalAndReplicationState() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));

        String completeReplicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        String incompleteReplicatorProducerName = REPLICATOR_PREFIX + ".c3-->c2";
        String entryOnlyProducerName = REPLICATOR_PREFIX + ".c4-->c2";
        Map<String, Long> oldSnapshot = new HashMap<>();
        oldSnapshot.put("normal-producer", 5L);
        oldSnapshot.put(completeReplicatorProducerName + "_LID", 7L);
        oldSnapshot.put(completeReplicatorProducerName + "_EID", 9L);
        oldSnapshot.put(incompleteReplicatorProducerName + "_LID", 11L);
        oldSnapshot.put(entryOnlyProducerName + "_EID", 12L);

        messageDeduplication.recoverDeduplicationStateFromSnapshot(oldSnapshot);

        assertEquals(messageDeduplication.highestSequencedPushed, oldSnapshot);
        MessageDeduplication.ReplSourcePosition expectedPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 9L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(completeReplicatorProducerName),
                expectedPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(completeReplicatorProducerName),
                expectedPosition);
        assertFalse(messageDeduplication.highestReplPositionPushed.containsKey(incompleteReplicatorProducerName));
        assertFalse(messageDeduplication.highestReplPositionPersisted.containsKey(incompleteReplicatorProducerName));
        assertFalse(messageDeduplication.highestReplPositionPushed.containsKey(entryOnlyProducerName));
        assertFalse(messageDeduplication.highestReplPositionPersisted.containsKey(entryOnlyProducerName));
    }

    @Test
    public void testRecoverOldSnapshotWithNormalAndReplicationStateHighestSequencedPersistedEquivalence() {
        // The historical snapshot keys are also loaded as raw sequence state, so the legacy V1 alias keys
        // must remain in highestSequencedPersisted after recovery.
        MessageDeduplication messageDeduplication = newEnabledMessageDeduplication();
        String replicatorProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        messageDeduplication.recoverDeduplicationStateFromSnapshot(Map.of(
                replicatorProducerName + "_LID", 7L,
                replicatorProducerName + "_EID", 9L));
        assertEquals(messageDeduplication.highestSequencedPersisted.get(replicatorProducerName + "_LID").value,
                7L);
        assertEquals(messageDeduplication.highestSequencedPersisted.get(replicatorProducerName + "_EID").value,
                9L);
    }

    @Test
    public void testReplayRecoversGeoReplicationWatermarkAcrossLedgers() {
        String clusterName = "c2";

        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setClusterName(clusterName);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        doReturn(Optional.empty()).when(topic).getShadowSourceTopic();

        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";

        MessageMetadata firstMetadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(1L)
                .setPublishTime(System.currentTimeMillis());
        firstMetadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");
        assertTrue(messageDeduplication.recoverReplWatermarkFromMetadata(firstMetadata));

        MessageMetadata nextMetadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(2L)
                .setPublishTime(System.currentTimeMillis());
        nextMetadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:10");
        assertTrue(messageDeduplication.recoverReplWatermarkFromMetadata(nextMetadata));

        MessageDeduplication.ReplSourcePosition nextPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 10L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(geoProducerName), nextPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), nextPosition);
        assertFalse(messageDeduplication.highestSequencedPushed.containsKey(geoProducerName));
        assertFalse(messageDeduplication.highestSequencedPersisted.containsKey(geoProducerName));

        MessageMetadata laterMetadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(3L)
                .setPublishTime(System.currentTimeMillis());
        laterMetadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("8:0");
        messageDeduplication.recoverDeduplicationStateFromMetadata(laterMetadata);

        MessageDeduplication.ReplSourcePosition laterPosition =
                new MessageDeduplication.ReplSourcePosition(8L, 0L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(geoProducerName), laterPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), laterPosition);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").value, 3L);
    }

    @Test
    public void testPersistedGeoReplicationWatermarkAcrossLedgers() {
        MessageDeduplication messageDeduplication = newEnabledMessageDeduplication();

        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        Topic.PublishContext publishContext = mock(Topic.PublishContext.class);
        doReturn(geoProducerName).when(publishContext).getProducerName();
        doReturn(new long[]{7L, 9L}, new long[]{7L, 10L}, new long[]{8L, 0L})
                .when(publishContext).getProperty(MSG_PROP_REPL_SOURCE_POSITION);
        Position position = PositionFactory.create(1L, 1L);

        messageDeduplication.recordMessagePersistedRepl(publishContext, position);
        messageDeduplication.recordMessagePersistedRepl(publishContext, position);
        MessageDeduplication.ReplSourcePosition nextPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 10L);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), nextPosition);

        messageDeduplication.recordMessagePersistedRepl(publishContext, position);
        MessageDeduplication.ReplSourcePosition laterPosition =
                new MessageDeduplication.ReplSourcePosition(8L, 0L);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), laterPosition);
    }

    @Test
    public void testPersistedWatermarksDoNotRegressWhenCallbacksCompleteOutOfOrder() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        Position laterPosition = PositionFactory.create(2L, 0L);
        Position earlierPosition = PositionFactory.create(1L, 9L);

        Topic.PublishContext normalContext = mock(Topic.PublishContext.class);
        doReturn("app-producer").when(normalContext).getProducerName();
        doReturn(10L, 9L).when(normalContext).getSequenceId();
        messageDeduplication.recordMessagePersistedNormal(normalContext, laterPosition);
        messageDeduplication.recordMessagePersistedNormal(normalContext, earlierPosition);
        // Normal sequence IDs keep the existing overwrite semantics: only replication positions, whose
        // property names the source position, are monotonic.
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").value, 9L);

        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        Topic.PublishContext replicationContext = mock(Topic.PublishContext.class);
        doReturn(geoProducerName).when(replicationContext).getProducerName();
        doReturn(new long[]{8L, 0L}, new long[]{7L, 99L})
                .when(replicationContext).getProperty(MSG_PROP_REPL_SOURCE_POSITION);
        messageDeduplication.recordMessagePersistedRepl(replicationContext, laterPosition);
        messageDeduplication.recordMessagePersistedRepl(replicationContext, earlierPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName),
                new MessageDeduplication.ReplSourcePosition(8L, 0L));
    }

    @Test
    public void testReplayRecoversMixedV1V2AndInvalidReplicationEntries() {
        String clusterName = "c2";

        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setClusterName(clusterName);
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        doReturn(Optional.empty()).when(topic).getShadowSourceTopic();
        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));

        MessageMetadata v1Metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(7L)
                .setHighestSequenceId(9L)
                .setPublishTime(System.currentTimeMillis());
        messageDeduplication.recoverDeduplicationStateFromMetadata(v1Metadata);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").value, 9L);

        MessageMetadata v2Metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(10L)
                .setPublishTime(System.currentTimeMillis());
        v2Metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");
        messageDeduplication.recoverDeduplicationStateFromMetadata(v2Metadata);
        // Source-position properties also occur in V1 records, so preserve the original sequence state.
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").value, 10L);

        // A producer name can be reused with lower sequence ids after its deduplication state is purged.
        MessageMetadata reusedV1Metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(8L)
                .setPublishTime(System.currentTimeMillis());
        messageDeduplication.recoverDeduplicationStateFromMetadata(reusedV1Metadata);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").value, 8L);

        MessageMetadata invalidV2Metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(11L)
                .setPublishTime(System.currentTimeMillis());
        invalidV2Metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("invalid");
        messageDeduplication.recoverDeduplicationStateFromMetadata(invalidV2Metadata);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").value, 11L);

        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        MessageDeduplication.ReplSourcePosition expectedPosition =
                new MessageDeduplication.ReplSourcePosition(7L, 9L);
        assertEquals(messageDeduplication.highestReplPositionPushed.get(geoProducerName), expectedPosition);
        assertEquals(messageDeduplication.highestReplPositionPersisted.get(geoProducerName), expectedPosition);
    }

    @Test
    public void testReplayRecoverGeoReplicationWatermarkSkipsMarkers() {
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);
        serviceConfiguration.setClusterName("c2");
        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();

        PersistentTopic topic = mock(PersistentTopic.class);
        doReturn("persistent://prop/ns/tp").when(topic).getName();
        doReturn(Optional.empty()).when(topic).getShadowSourceTopic();

        MessageDeduplication messageDeduplication = new MessageDeduplication(pulsarService, topic,
                mock(ManagedLedger.class));
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("app-producer")
                .setReplicatedFrom("c1")
                .setSequenceId(1L)
                .setMarkerType(MarkerType.REPLICATED_SUBSCRIPTION_UPDATE_VALUE)
                .setPublishTime(System.currentTimeMillis());
        metadata.addProperty().setKey(MSG_PROP_REPL_SOURCE_POSITION).setValue("7:9");

        messageDeduplication.recoverDeduplicationStateFromMetadata(metadata);

        String geoProducerName = REPLICATOR_PREFIX + ".c1-->c2";
        assertFalse(messageDeduplication.highestReplPositionPushed.containsKey(geoProducerName));
        assertFalse(messageDeduplication.highestReplPositionPersisted.containsKey(geoProducerName));
        assertEquals(messageDeduplication.highestSequencedPushed.get("app-producer").longValue(), 1L);
        assertEquals(messageDeduplication.highestSequencedPersisted.get("app-producer").value, 1L);
    }

    @Test
    public void testIsDuplicateWithFailure() {

        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setBrokerDeduplicationEntriesInterval(BROKER_DEDUPLICATION_ENTRIES_INTERVAL);
        serviceConfiguration.setBrokerDeduplicationMaxNumberOfProducers(BROKER_DEDUPLICATION_MAX_NUMBER_PRODUCERS);
        serviceConfiguration.setReplicatorPrefix(REPLICATOR_PREFIX);

        doReturn(serviceConfiguration).when(pulsarService).getConfiguration();
        doReturn(mock(PulsarResources.class)).when(pulsarService).getPulsarResources();
        doReturn(mock(CompactionServiceFactory.class)).when(pulsarService).getCompactionServiceFactory();

        ManagedLedger managedLedger = mock(ManagedLedger.class);
        MessageDeduplication messageDeduplication = spy(new MessageDeduplication(pulsarService,
                mock(PersistentTopic.class), managedLedger));
        doReturn(true).when(messageDeduplication).isEnabled();

        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);

        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            Runnable test = (Runnable) args[0];
            test.run();
            return null;
        }).when(eventLoopGroup).submit(any(Runnable.class));

        BrokerService brokerService = mock(BrokerService.class);
        doReturn(eventLoopGroup).when(brokerService).executor();
        doReturn(pulsarService).when(brokerService).pulsar();
        doReturn(pulsarService).when(brokerService).getPulsar();
        doReturn(new BacklogQuotaManager(pulsarService)).when(brokerService).getBacklogQuotaManager();
        doReturn(AsyncTokenBucket.DEFAULT_SNAPSHOT_CLOCK).when(pulsarService).getMonotonicClock();

        PersistentTopic persistentTopic = spyWithClassAndConstructorArgs(PersistentTopic.class,
                "topic-1", brokerService, managedLedger, messageDeduplication);

        String producerName1 = "producer1";
        ByteBuf byteBuf1 = getMessage(producerName1, 0);
        Topic.PublishContext publishContext1 = getPublishContext(producerName1, 0);

        String producerName2 = "producer2";
        ByteBuf byteBuf2 = getMessage(producerName2, 1);
        Topic.PublishContext publishContext2 = getPublishContext(producerName2, 1);

        persistentTopic.publishMessage(byteBuf1, publishContext1);
        persistentTopic.addComplete(PositionFactory.create(0, 1), null, publishContext1);
        verify(managedLedger, times(1)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        Long lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 0);
        lastSequenceIdPushed = messageDeduplication.getHighestSequencedPersisted(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 0);

        persistentTopic.publishMessage(byteBuf2, publishContext2);
        persistentTopic.addComplete(PositionFactory.create(0, 2), null, publishContext2);
        verify(managedLedger, times(2)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        byteBuf2.release();

        lastSequenceIdPushed = messageDeduplication.getHighestSequencedPersisted(producerName2);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        byteBuf1.release();

        byteBuf1 = getMessage(producerName1, 1);
        publishContext1 = getPublishContext(producerName1, 1);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        persistentTopic.addComplete(PositionFactory.create(0, 3), null, publishContext1);
        verify(managedLedger, times(3)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        lastSequenceIdPushed = messageDeduplication.getHighestSequencedPersisted(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        byteBuf1.release();

        byteBuf1 = getMessage(producerName1, 5);
        publishContext1 = getPublishContext(producerName1, 5);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        persistentTopic.addComplete(PositionFactory.create(0, 4), null, publishContext1);
        verify(managedLedger, times(4)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
        lastSequenceIdPushed = messageDeduplication.getHighestSequencedPersisted(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
        byteBuf1.release();

        // publish dup
        byteBuf1 = getMessage(producerName1, 0);
        publishContext1 = getPublishContext(producerName1, 0);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(4)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
        verify(publishContext1, times(1)).completed(eq(null), eq(-1L), eq(-1L));
        byteBuf1.release();

        // publish message unknown dup status
        byteBuf1 = getMessage(producerName1, 6);
        publishContext1 = getPublishContext(producerName1, 6);
        // don't complete message
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(5)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 6);
        lastSequenceIdPushed = messageDeduplication.getHighestSequencedPersisted(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 5);
        byteBuf1.release();

        // publish same message again
        byteBuf1 = getMessage(producerName1, 6);
        publishContext1 = getPublishContext(producerName1, 6);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(5)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        verify(publishContext1, times(1))
                .completed(any(MessageDeduplication.MessageDupUnknownException.class), eq(-1L), eq(-1L));

        // complete seq 6 message eventually
        persistentTopic.addComplete(PositionFactory.create(0, 5), null, publishContext1);
        byteBuf1.release();

        // simulate failure
        byteBuf1 = getMessage(producerName1, 7);
        publishContext1 = getPublishContext(producerName1, 7);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(6)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());

        persistentTopic.addFailed(new ManagedLedgerException("test"), publishContext1);
        // check highestSequencedPushed is reset
        assertEquals(messageDeduplication.highestSequencedPushed.size(), 2);
        assertEquals(messageDeduplication.highestSequencedPersisted.size(), 2);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertEquals(lastSequenceIdPushed.longValue(), 6);
        lastSequenceIdPushed = messageDeduplication.getHighestSequencedPersisted(producerName1);
        assertEquals(lastSequenceIdPushed.longValue(), 6);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName2);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        lastSequenceIdPushed = messageDeduplication.getHighestSequencedPersisted(producerName2);
        assertEquals(lastSequenceIdPushed.longValue(), 1);
        verify(messageDeduplication, times(1)).resetHighestSequenceIdPushed();
        byteBuf1.release();

        // try dup
        byteBuf1 = getMessage(producerName1, 6);
        publishContext1 = getPublishContext(producerName1, 6);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(6)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        verify(publishContext1, times(1)).completed(eq(null), eq(-1L), eq(-1L));
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 6);
        byteBuf1.release();

        // try new message
        byteBuf1 = getMessage(producerName1, 8);
        publishContext1 = getPublishContext(producerName1, 8);
        persistentTopic.publishMessage(byteBuf1, publishContext1);
        verify(managedLedger, times(7)).asyncAddEntry(any(ByteBuf.class), anyInt(), any(), any());
        persistentTopic.addComplete(PositionFactory.create(0, 5), null, publishContext1);
        lastSequenceIdPushed = messageDeduplication.highestSequencedPushed.get(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 8);
        lastSequenceIdPushed = messageDeduplication.getHighestSequencedPersisted(producerName1);
        assertNotNull(lastSequenceIdPushed);
        assertEquals(lastSequenceIdPushed.longValue(), 8);
        byteBuf1.release();

    }

    public ByteBuf getMessage(String producerName, long seqId) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setProducerName(producerName)
                .setSequenceId(seqId)
                .setPublishTime(System.currentTimeMillis());

        ByteBuf payload = Unpooled.copiedBuffer(new byte[0]);
        ByteBuf byteBuf = serializeMetadataAndPayload(
                Commands.ChecksumType.Crc32c, messageMetadata, payload);
        payload.release();
        return byteBuf;
    }

    public Topic.PublishContext getPublishContext(String producerName, long seqId) {
        return spy(new Topic.PublishContext() {
            @Override
            public String getProducerName() {
                return producerName;
            }

            public long getSequenceId() {
                return seqId;
            }

            @Override
            public void completed(Exception e, long ledgerId, long entryId) {

            }
        });
    }

    public Topic.PublishContext getPublishContext(String producerName, long seqId, long lastSequenceId) {
        return spy(new Topic.PublishContext() {
            @Override
            public String getProducerName() {
                return producerName;
            }

            @Override
            public long getSequenceId() {
                return seqId;
            }

            @Override
            public long getHighestSequenceId() {
                return lastSequenceId;
            }

            @Override
            public void completed(Exception e, long ledgerId, long entryId) {

            }
        });
    }

    // The test methods share the broker (most of them don't use it) and use topics of their own. After a failed
    // method, the next one gets a new broker (TestRetrySupport).
    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.conf.setBrokerDeduplicationEnabled(true);
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMessageDeduplication() throws Exception {
        String topicName = "persistent://prop/ns-abc/testMessageDeduplication";
        String producerName = "test-producer";
        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .producerName(producerName)
                .topic(topicName)
                .create();
        final PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicName).get().orElse(null);
        assertNotNull(persistentTopic);
        final MessageDeduplication messageDeduplication = persistentTopic.getMessageDeduplication();
        assertFalse(messageDeduplication.getInactiveProducers().containsKey(producerName));
        producer.close();
        Awaitility.await().untilAsserted(() -> assertTrue(messageDeduplication
                .getInactiveProducers().containsKey(producerName)));
        admin.topicPolicies().setDeduplicationStatus(topicName, false);
        Awaitility.await().untilAsserted(() -> {
                    final Boolean deduplicationStatus = admin.topicPolicies().getDeduplicationStatus(topicName);
                    Assert.assertNotNull(deduplicationStatus);
                    Assert.assertFalse(deduplicationStatus);
                });
        messageDeduplication.purgeInactiveProducers();
        assertTrue(messageDeduplication.getInactiveProducers().isEmpty());
    }

    @Test(dataProvider = "trueFalse", timeOut = 60000)
    public void testReconnectDuringInactiveProducerPurge(boolean disconnectAfterReconnect) throws Exception {
        String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
        String producerName = "reconnecting-producer";
        try (Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topicName)
                .startMessageId(MessageId.earliest).create()) {
            try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                    .producerName(producerName).enableBatching(false).create()) {
                producer.newMessage().sequenceId(5).value("before-reconnect").send();
            }
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName)
                    .get().orElseThrow();
            MessageDeduplication deduplication = topic.getMessageDeduplication();
            Awaitility.await().until(() -> {
                Long inactiveSince = deduplication.getInactiveProducers().get(producerName);
                return inactiveSince != null && inactiveSince < System.currentTimeMillis();
            });
            long originalInactiveSince = deduplication.getInactiveProducers().get(producerName);

            CountDownLatch candidateSelected = new CountDownLatch(1);
            CountDownLatch resumePurge = new CountDownLatch(1);
            // Pause the actual purge after it reads the expired entry, without replacing its maps or callbacks.
            AbstractAppender appender = new AbstractAppender("pause-dedup-purge", null,
                    PatternLayout.createDefaultLayout(), false, null) {
                @Override
                public void append(LogEvent event) {
                    if (event.getLoggerName().equals(MessageDeduplication.class.getName())
                            && event.getMessage().getFormattedMessage()
                                    .equals("Purging dedup information for producer")
                            && producerName.equals(event.getContextData().getValue("producerName"))) {
                        candidateSelected.countDown();
                        try {
                            assertTrue(resumePurge.await(30, TimeUnit.SECONDS), "Purge was not resumed");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                }
            };
            Logger logger = (Logger) LogManager.getLogger(MessageDeduplication.class);
            ExecutorService executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("dedup-purge-test"));
            int inactivityTimeout = conf.getBrokerDeduplicationProducerInactivityTimeoutMinutes();
            appender.start();
            logger.addAppender(appender);
            conf.setBrokerDeduplicationProducerInactivityTimeoutMinutes(0);
            try {
                Future<?> purge = executor.submit(topic::checkMessageDeduplicationInfo);
                assertTrue(candidateSelected.await(30, TimeUnit.SECONDS), "Purge did not select the expired producer");
                try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                        .producerName(producerName).enableBatching(false).create()) {
                    producer.newMessage().sequenceId(6).value("after-reconnect").send();
                    assertFalse(deduplication.getInactiveProducers().containsKey(producerName));
                    Long refreshedInactiveSince = null;
                    if (disconnectAfterReconnect) {
                        producer.close();
                        Awaitility.await().untilAsserted(() -> {
                            Long inactiveSince = deduplication.getInactiveProducers().get(producerName);
                            assertNotNull(inactiveSince);
                            assertTrue(inactiveSince > originalInactiveSince,
                                    "The replacement producer must have a refreshed inactivity timestamp");
                        });
                        refreshedInactiveSince = deduplication.getInactiveProducers().get(producerName);
                    }
                    resumePurge.countDown();
                    purge.get(30, TimeUnit.SECONDS);

                    if (disconnectAfterReconnect) {
                        assertEquals(deduplication.getInactiveProducers().get(producerName), refreshedInactiveSince,
                                "The stale purge must preserve the refreshed inactivity record");
                        try (Producer<String> retryProducer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                                .producerName(producerName).enableBatching(false).create()) {
                            assertEquals(retryProducer.newMessage().sequenceId(6).value("retry").send(),
                                    new MessageIdImpl(-1, -1, -1),
                                    "A retry after reconnect and disconnect must still be deduplicated");
                        }
                    } else {
                        assertEquals(producer.newMessage().sequenceId(6).value("retry").send(),
                                new MessageIdImpl(-1, -1, -1), "A retry after reconnect must still be deduplicated");
                    }
                    assertEquals(reader.readNext(10, TimeUnit.SECONDS).getValue(), "before-reconnect");
                    assertEquals(reader.readNext(10, TimeUnit.SECONDS).getValue(), "after-reconnect");
                    assertFalse(reader.hasMessageAvailable(), "The retry must not add another stored message");
                }
            } finally {
                resumePurge.countDown();
                executor.shutdownNow();
                conf.setBrokerDeduplicationProducerInactivityTimeoutMinutes(inactivityTimeout);
                logger.removeAppender(appender);
                appender.stop();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
        }
    }

    @Test(timeOut = 60000)
    public void testSnapshotAfterDeduplicationIsReenabled() throws Exception {
        String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
        String producerName = "snapshot-producer";
        int snapshotInterval = conf.getBrokerDeduplicationEntriesInterval();
        // Keep the initial publish below the entry threshold so the interval-triggered snapshot can be paused.
        // After re-enabling, the next two publishes trigger an entry-count snapshot.
        conf.setBrokerDeduplicationEntriesInterval(2);
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .producerName(producerName).enableBatching(false).create()) {
            producer.newMessage().sequenceId(0).value("before-disable").send();
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName)
                    .get().orElseThrow();
            MessageDeduplication deduplication = topic.getMessageDeduplication();
            admin.topicPolicies().setDeduplicationSnapshotInterval(topicName, 1);
            Awaitility.await().untilAsserted(() -> assertEquals(
                    topic.getHierarchyTopicPolicies().getDeduplicationSnapshotIntervalSeconds().get(),
                    Integer.valueOf(1)));

            CountDownLatch snapshotStarted = new CountDownLatch(1);
            CountDownLatch resumeSnapshot = new CountDownLatch(1);
            CountDownLatch cursorUnavailable = new CountDownLatch(1);
            AtomicBoolean pauseSnapshot = new AtomicBoolean(true);
            // Pause the monitor's production entry point after its enabled/cursor checks, then disable via Admin.
            AbstractAppender appender = new AbstractAppender("pause-dedup-snapshot", null,
                    PatternLayout.createDefaultLayout(), false, null) {
                @Override
                public void append(LogEvent event) {
                    if (!event.getLoggerName().equals(MessageDeduplication.class.getName())
                            || !topicName.equals(event.getContextData().getValue("topic"))) {
                        return;
                    }
                    // These messages come from MessageDeduplication.takeSnapshot(Position); keep the matchers in sync.
                    String message = event.getMessage().getFormattedMessage();
                    if (message.equals("Taking snapshot of sequence ids map")
                            && pauseSnapshot.compareAndSet(true, false)) {
                        snapshotStarted.countDown();
                        try {
                            assertTrue(resumeSnapshot.await(30, TimeUnit.SECONDS), "Snapshot was not resumed");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    } else if (message.equals("Cursor is null when taking snapshot for")) {
                        cursorUnavailable.countDown();
                    }
                }
            };
            Logger logger = (Logger) LogManager.getLogger(MessageDeduplication.class);
            Level originalLevel = logger.getLevel();
            ExecutorService executor = Executors.newSingleThreadExecutor(
                    new DefaultThreadFactory("dedup-snapshot-test"));
            appender.start();
            logger.addAppender(appender);
            Configurator.setLevel(logger.getName(), Level.DEBUG);
            try {
                Future<?> snapshot = executor.submit(topic::checkDeduplicationSnapshot);
                assertTrue(snapshotStarted.await(30, TimeUnit.SECONDS), "Snapshot did not start");
                admin.topicPolicies().setDeduplicationStatus(topicName, false);
                Awaitility.await().until(() -> deduplication.getStatus() == MessageDeduplication.Status.Disabled
                        && deduplication.getManagedCursor() == null);
                resumeSnapshot.countDown();
                snapshot.get(30, TimeUnit.SECONDS);
                assertEquals(cursorUnavailable.getCount(), 0L, "Snapshot must encounter the deleted cursor");

                admin.topicPolicies().setDeduplicationStatus(topicName, true);
                Awaitility.await().until(deduplication::isEnabled);
                ManagedCursor cursor = deduplication.getManagedCursor();
                producer.newMessage().sequenceId(1).value("after-enable-1").send();
                producer.newMessage().sequenceId(2).value("after-enable-2").send();
                Position lastPosition = topic.getManagedLedger().getLastConfirmedEntry();
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertEquals(cursor.getMarkDeletedPosition(), lastPosition,
                            "Reenabled deduplication must resume snapshot progress");
                    assertEquals(cursor.getProperties(), Map.of(producerName, 2L));
                });
            } finally {
                resumeSnapshot.countDown();
                executor.shutdownNow();
                logger.removeAppender(appender);
                Configurator.setLevel(logger.getName(), originalLevel);
                appender.stop();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
        } finally {
            conf.setBrokerDeduplicationEntriesInterval(snapshotInterval);
        }
    }

    // The REST produce API publishes without a producer. The topic doesn't deduplicate such a publish, since it tracks
    // the sequence IDs per producer, but counts its entry toward the snapshot interval
    @Test(timeOut = 60000)
    public void testPublishWithoutProducer() throws Exception {
        String topicName = "persistent://prop/ns-abc/" + UUID.randomUUID();
        int snapshotInterval = conf.getBrokerDeduplicationEntriesInterval();
        conf.setBrokerDeduplicationEntriesInterval(2);
        try {
            admin.topics().createNonPartitionedTopic(topicName);
            PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName)
                    .get().orElseThrow();
            MessageDeduplication deduplication = topic.getMessageDeduplication();
            Awaitility.await().until(deduplication::isEnabled);

            // Both publishes are persisted, though with the same sequence ID, and the second one reaches the
            // snapshot interval
            publish(topic, null, 1).get(10, TimeUnit.SECONDS);
            Position position = publish(topic, null, 1).get(10, TimeUnit.SECONDS);
            assertEquals(topic.getNumberOfEntries(), 2);
            assertTrue(deduplication.highestSequencedPushed.isEmpty());
            assertTrue(deduplication.highestSequencedPersisted.isEmpty());
            ManagedCursor cursor = deduplication.getManagedCursor();
            Awaitility.await().untilAsserted(() -> assertEquals(cursor.getMarkDeletedPosition(), position));

            // A producer's duplicate is still dropped: acknowledged with the position -1:-1, without being persisted
            publish(topic, "producer", 1).get(10, TimeUnit.SECONDS);
            assertEquals(publish(topic, "producer", 1).get(10, TimeUnit.SECONDS), PositionFactory.create(-1, -1));
            assertEquals(topic.getNumberOfEntries(), 3);
        } finally {
            conf.setBrokerDeduplicationEntriesInterval(snapshotInterval);
        }
    }

    // Publishes a message from the producer, or without a producer when the name is null, and returns the position
    // the topic acknowledges it with
    private CompletableFuture<Position> publish(PersistentTopic topic, String producerName, long sequenceId) {
        CompletableFuture<Position> position = new CompletableFuture<>();
        ByteBuf message = getMessage(producerName != null ? producerName : "RestProducer", sequenceId);
        try {
            topic.publishMessage(message, new Topic.PublishContext() {
                @Override
                public String getProducerName() {
                    return producerName;
                }

                @Override
                public long getSequenceId() {
                    return sequenceId;
                }

                @Override
                public void completed(Exception e, long ledgerId, long entryId) {
                    if (e != null) {
                        position.completeExceptionally(e);
                    } else {
                        position.complete(PositionFactory.create(ledgerId, entryId));
                    }
                }
            });
        } finally {
            message.release();
        }
        return position;
    }

    @Test
    public void testMessageDeduplicationShouldNotWorkForSystemTopic() throws PulsarAdminException {
        final String localName = UUID.randomUUID().toString();
        final String namespace = "prop/ns-abc";
        final String prefix = "persistent://%s/".formatted(namespace);
        final String topic = prefix + localName;
        admin.topics().createNonPartitionedTopic(topic);

        // broker level policies
        final String eventSystemTopic = prefix + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
        final Optional<Topic> optionalTopic = pulsar.getBrokerService().getTopic(eventSystemTopic, true).join();
        assertTrue(optionalTopic.isPresent());
        final Topic ptRef = optionalTopic.get();
        assertTrue(ptRef.isSystemTopic());
        assertFalse(ptRef.isDeduplicationEnabled());

        // namespace level policies
        admin.namespaces().setDeduplicationStatus(namespace, true);
        assertTrue(ptRef.isSystemTopic());
        assertFalse(ptRef.isDeduplicationEnabled());

        // topic level policies
        admin.topicPolicies().setDeduplicationStatus(eventSystemTopic, true);
        assertTrue(ptRef.isSystemTopic());
        assertFalse(ptRef.isDeduplicationEnabled());
    }
}
