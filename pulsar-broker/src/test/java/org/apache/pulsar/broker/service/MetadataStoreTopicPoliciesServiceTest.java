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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests of {@link MetadataStoreTopicPoliciesService} against a real metadata store, covering the changes that produce
 * no notification because the session with the store was lost (issue #26453).
 */
@Test(groups = "broker")
public class MetadataStoreTopicPoliciesServiceTest {

    private static final TopicName TOPIC = TopicName.get("persistent://tenant/ns/topic");
    private static final TopicName OTHER_TOPIC = TopicName.get("persistent://tenant/ns/other-topic");

    private SessionFiringMetadataStore store;
    private MetadataStoreTopicPoliciesService service;
    // TestNG reuses the instance across methods, so these are cleared in setUp.
    private final Map<TopicName, List<TopicPolicies>> received = new ConcurrentHashMap<>();
    private final List<Throwable> listenerErrors = new CopyOnWriteArrayList<>();

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        received.clear();
        listenerErrors.clear();
        store = new SessionFiringMetadataStore();
        PulsarService pulsar = mock(PulsarService.class);
        // The same store backs both scopes, like a single-cluster deployment.
        when(pulsar.getLocalMetadataStore()).thenReturn(store);
        when(pulsar.getConfigurationMetadataStore()).thenReturn(store);
        service = new MetadataStoreTopicPoliciesService();
        service.start(pulsar);
        registerListener(TOPIC);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        service.close();
        store.close();
    }

    @Test
    public void testUpdateAndDeleteAreDeliveredByNotification() throws Exception {
        setPolicies(false, 100L);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(lastThreshold(TOPIC)).isEqualTo(100L));

        service.deleteTopicPoliciesAsync(TOPIC).get(5, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received(TOPIC)).last().isNull());
        assertThat(listenerErrors).isEmpty();
    }

    /**
     * A policy changed while the session with the store is lost produces no notification, so the listeners must be
     * told about the new value when the session is re-established.
     */
    @Test
    public void testChangeWhileDisconnectedIsDeliveredAfterReconnection() throws Exception {
        setPolicies(false, 100L);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(lastThreshold(TOPIC)).isEqualTo(100L));

        store.setNotificationsEnabled(false);
        setPolicies(false, 200L);
        store.setNotificationsEnabled(true);
        assertThat(lastThreshold(TOPIC)).isEqualTo(100L);

        store.fireSessionEvent(SessionEvent.Reconnected);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(lastThreshold(TOPIC)).isEqualTo(200L));
        assertThat(listenerErrors).isEmpty();
    }

    /**
     * A policy deleted while the session was lost produces no notification either, so the listeners must be told
     * that the policies are gone when the session is re-established.
     */
    @Test
    public void testDeleteWhileDisconnectedIsDeliveredAfterReconnection() throws Exception {
        setPolicies(false, 100L);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(lastThreshold(TOPIC)).isEqualTo(100L));

        store.setNotificationsEnabled(false);
        service.deleteTopicPoliciesAsync(TOPIC).get(5, TimeUnit.SECONDS);
        store.setNotificationsEnabled(true);
        assertThat(lastThreshold(TOPIC)).isEqualTo(100L);

        store.fireSessionEvent(SessionEvent.SessionReestablished);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(received(TOPIC)).last().isNull());
        assertThat(listenerErrors).isEmpty();
    }

    /**
     * A topic that never had policies in a scope must not be told that they were deleted when the session is
     * re-established: a {@code null} notification is indistinguishable from a deletion for a topic that is still
     * loading its initial policies.
     */
    @Test
    public void testTopicsWithoutPoliciesAreNotNotifiedAsDeleted() throws Exception {
        registerListener(OTHER_TOPIC);

        store.fireSessionEvent(SessionEvent.Reconnected);

        // Give the resync the chance to notify before asserting that it did not.
        await().pollDelay(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(received(OTHER_TOPIC)).isEmpty());
        assertThat(listenerErrors).isEmpty();
    }

    /**
     * The global policies are re-read when a single store backs both scopes, and they are re-read once per session
     * event: registering a session listener per scope on the same store would resync it twice.
     */
    @Test
    public void testGlobalPoliciesAreResyncedOnceWithASingleStore() throws Exception {
        service.updateTopicPoliciesAsync(TOPIC, true, false, p -> p.setCompactionThreshold(300L))
                .get(5, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(received(TOPIC)).isNotEmpty().last()
                        .extracting(TopicPolicies::isGlobalPolicies).isEqualTo(true));

        store.setNotificationsEnabled(false);
        service.updateTopicPoliciesAsync(TOPIC, true, false, p -> p.setCompactionThreshold(400L))
                .get(5, TimeUnit.SECONDS);
        store.setNotificationsEnabled(true);

        store.fireSessionEvent(SessionEvent.Reconnected);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(globalThresholds(400L)).hasSize(1));
        // Wait for a duplicate delivery, if any, before asserting there is none.
        await().pollDelay(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(globalThresholds(400L)).hasSize(1));
        assertThat(listenerErrors).isEmpty();
    }

    /**
     * The local policies are re-read once per session event, not once per scope of the single store that backs both.
     */
    @Test
    public void testLocalPoliciesAreResyncedOnceWithASingleStore() throws Exception {
        setPolicies(false, 100L);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(lastThreshold(TOPIC)).isEqualTo(100L));

        store.fireSessionEvent(SessionEvent.Reconnected);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(localThresholds(100L)).hasSize(2));
        // Wait for a duplicate delivery, if any, before asserting there is none.
        await().pollDelay(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(localThresholds(100L)).hasSize(2));
        assertThat(listenerErrors).isEmpty();
    }

    private void registerListener(TopicName topicName) throws Exception {
        List<TopicPolicies> topicReceived = received.computeIfAbsent(topicName, __ -> new CopyOnWriteArrayList<>());
        service.registerListenerAsync(topicName, policies -> {
            try {
                topicReceived.add(policies);
            } catch (Throwable error) {
                listenerErrors.add(error);
            }
        }).get(5, TimeUnit.SECONDS);
    }

    private List<TopicPolicies> received(TopicName topicName) {
        return received.getOrDefault(topicName, List.of());
    }

    private Long lastThreshold(TopicName topicName) {
        List<TopicPolicies> topicReceived = received(topicName);
        TopicPolicies last = topicReceived.isEmpty() ? null : topicReceived.get(topicReceived.size() - 1);
        return last == null ? null : last.getCompactionThreshold();
    }

    private List<TopicPolicies> localThresholds(long threshold) {
        return received(TOPIC).stream()
                .filter(p -> p != null && !p.isGlobalPolicies() && p.getCompactionThreshold() == threshold)
                .toList();
    }

    private List<TopicPolicies> globalThresholds(long threshold) {
        return received(TOPIC).stream()
                .filter(p -> p != null && p.isGlobalPolicies() && p.getCompactionThreshold() == threshold)
                .toList();
    }

    private void setPolicies(boolean isGlobal, long compactionThreshold) throws Exception {
        service.updateTopicPoliciesAsync(TOPIC, isGlobal, false, p -> p.setCompactionThreshold(compactionThreshold))
                .get(5, TimeUnit.SECONDS);
    }

    /**
     * A local metadata store whose session events can be fired on demand and whose notifications can be dropped, to
     * reproduce a change that happens while the session with the store is lost.
     */
    private static final class SessionFiringMetadataStore extends LocalMemoryMetadataStore {
        private volatile boolean notificationsEnabled = true;

        SessionFiringMetadataStore() throws Exception {
            super("memory:local", MetadataStoreConfig.builder().build());
        }

        void setNotificationsEnabled(boolean notificationsEnabled) {
            this.notificationsEnabled = notificationsEnabled;
        }

        void fireSessionEvent(SessionEvent event) {
            receivedSessionEvent(event);
        }

        @Override
        protected CompletableFuture<Void> receivedNotification(Notification notification) {
            if (notificationsEnabled) {
                return super.receivedNotification(notification);
            }
            // Dropped: the change happened while the session was lost.
            return CompletableFuture.completedFuture(null);
        }
    }
}
