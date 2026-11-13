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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.jspecify.annotations.Nullable;

/**
 * Topic policies service backed by Pulsar metadata stores.
 */
@CustomLog
public class MetadataStoreTopicPoliciesService implements TopicPoliciesService {

    public static final String GLOBAL_POLICIES_ROOT = "/admin/topic-policies/global";
    public static final String LOCAL_POLICIES_ROOT = "/admin/topic-policies/local";

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<TopicName, TopicListeners> listeners = new ConcurrentHashMap<>();
    private MetadataCache<TopicPolicies> localPoliciesCache;
    private MetadataCache<TopicPolicies> globalPoliciesCache;
    private volatile Runnable cancelLocalStoreListener;
    private volatile Runnable cancelConfigurationStoreListener;
    private volatile Runnable cancelLocalSessionListener;
    private volatile Runnable cancelConfigurationSessionListener;

    @Override
    public void start(PulsarService pulsar) {
        MetadataStore localStore = pulsar.getLocalMetadataStore();
        MetadataStore configurationStore = pulsar.getConfigurationMetadataStore();
        this.localPoliciesCache = localStore.getMetadataCache(TopicPolicies.class);
        this.globalPoliciesCache = configurationStore.getMetadataCache(TopicPolicies.class);
        cancelLocalStoreListener =
                localStore.registerCancellableListener(notification -> handleNotification(notification, false));
        cancelConfigurationStoreListener =
                configurationStore.registerCancellableListener(notification -> handleNotification(notification, true));
        if (localStore == configurationStore) {
            // A single metadata store backs both scopes: one session listener has to resync both of them, otherwise
            // the same store would be resynced twice for every session event.
            cancelLocalSessionListener = registerSessionListener(localStore, true);
        } else {
            cancelLocalSessionListener = registerSessionListener(localStore, false);
            cancelConfigurationSessionListener = registerSessionListener(configurationStore, false);
        }
    }

    /**
     * Register a listener that re-reads the policies after the session with the metadata store has been
     * re-established.
     *
     * <p>This backend learns about changes only from metadata-store watches, which do not replay what happened while
     * the session was lost: a policy changed during a transient disconnection would never reach the listeners (issue
     * #26453). Re-reading the stored state on reconnection is the equivalent of
     * {@code SystemTopicBasedTopicPoliciesService} resuming its {@code __change_events} reader from the last
     * consumed offset.
     *
     * @param bothScopes whether the store backs both the local and the global policies (single-store deployment)
     */
    private Runnable registerSessionListener(MetadataStore store, boolean bothScopes) {
        if (!(store instanceof MetadataStoreExtended storeExtended)) {
            return () -> { };
        }
        return storeExtended.registerCancellableSessionListener(event -> {
            if (event != SessionEvent.Reconnected && event != SessionEvent.SessionReestablished) {
                return;
            }
            resyncTopicPolicies(false);
            if (bothScopes) {
                resyncTopicPolicies(true);
            }
        });
    }

    /**
     * Re-read the policies of every topic that has a listener registered and deliver them, so that the changes that
     * produced no notification while the session was disconnected are not lost.
     *
     * <p>A listener is told that the policies were removed ({@code null}) only when it may have been told that they
     * existed, i.e. when this scope had already delivered a value for the topic. Telling a topic that never had
     * policies in this scope that they were removed is indistinguishable from a deletion to a topic that is still
     * loading its initial policies, which would then be dropped.
     */
    private void resyncTopicPolicies(boolean isGlobalPolicy) {
        if (closed.get()) {
            return;
        }
        MetadataCache<TopicPolicies> cache = cache(isGlobalPolicy);
        for (TopicName topicName : listeners.keySet()) {
            String path = pathFor(topicName, isGlobalPolicy);
            // The store discards its caches on reconnection, so the path is read from the store, not from the cache.
            cache.invalidate(path);
            cache.get(path).whenComplete((policies, error) -> {
                if (error != null) {
                    log.warn()
                            .attr("path", path)
                            .exception(error)
                            .log("Failed to re-read topic policies after a metadata session event");
                    return;
                }
                if (policies.isPresent()) {
                    notifyListeners(topicName, isGlobalPolicy, cloneWithScope(policies.get(), isGlobalPolicy));
                } else if (isDelivered(topicName, isGlobalPolicy)) {
                    notifyListeners(topicName, isGlobalPolicy, null);
                }
            });
        }
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
        return deleteTopicPoliciesAsync(topicName, false);
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName,
                                                            boolean keepGlobalPoliciesAfterDeleting) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        if (NamespaceService.isHeartbeatNamespace(partitionedTopicName.getNamespaceObject())) {
            return CompletableFuture.completedFuture(null);
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new BrokerServiceException(getClass().getName() + " is closed."));
        }
        CompletableFuture<Void> deleteLocal =
                deleteIfExists(localPoliciesCache, pathFor(partitionedTopicName, false));
        if (keepGlobalPoliciesAfterDeleting) {
            return deleteLocal;
        }
        CompletableFuture<Void> deleteGlobal =
                deleteIfExists(globalPoliciesCache, pathFor(partitionedTopicName, true));
        return CompletableFuture.allOf(deleteLocal, deleteGlobal);
    }

    @Override
    public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, boolean isGlobalPolicy,
                                                            boolean skipUpdateWhenTopicPolicyDoesntExist,
                                                            Consumer<TopicPolicies> policyUpdater) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        if (NamespaceService.isHeartbeatNamespace(partitionedTopicName.getNamespaceObject())) {
            return CompletableFuture.failedFuture(new BrokerServiceException.NotAllowedException(
                    "Not allowed to update topic policy for the heartbeat topic"));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new BrokerServiceException(getClass().getName() + " is closed."));
        }
        MetadataCache<TopicPolicies> cache = cache(isGlobalPolicy);
        String path = pathFor(partitionedTopicName, isGlobalPolicy);
        CompletableFuture<TopicPolicies> updateFuture;
        if (skipUpdateWhenTopicPolicyDoesntExist) {
            updateFuture = cache.readModifyUpdate(path,
                    current -> updatePolicies(Optional.of(current), isGlobalPolicy, policyUpdater));
        } else {
            updateFuture = cache.readModifyUpdateOrCreate(path,
                    current -> updatePolicies(current, isGlobalPolicy, policyUpdater));
        }
        return updateFuture.thenAccept(__ -> { }).exceptionally(error -> {
            if (skipUpdateWhenTopicPolicyDoesntExist
                    && FutureUtil.unwrapCompletionException(error) instanceof NotFoundException) {
                return null;
            }
            throw FutureUtil.wrapToCompletionException(error);
        });
    }

    @Override
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        if (NamespaceService.isHeartbeatNamespace(partitionedTopicName.getNamespaceObject())) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (closed.get()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        boolean global = type == GetType.GLOBAL_ONLY;
        return cache(global).get(pathFor(partitionedTopicName, global)).thenApply(policies -> {
            // A topic that loaded its policies also knows about them, even though no notification was delivered for
            // them, so a later resync has to tell it if they are gone. The state is kept with the listeners, so
            // reading the policies of a topic without one leaves nothing behind.
            if (policies.isPresent()) {
                markDelivered(partitionedTopicName, global);
            }
            return policies.map(policy -> cloneWithScope(policy, global));
        });
    }

    @Deprecated
    @Override
    public boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
        listeners.compute(normalizeTopicName(topicName), (__, topicListeners) -> {
            if (topicListeners == null) {
                topicListeners = new TopicListeners();
            }
            topicListeners.listeners.add(listener);
            return topicListeners;
        });
        return true;
    }

    @Override
    public void unregisterListener(TopicName topicName, TopicPolicyListener listener) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        listeners.computeIfPresent(partitionedTopicName, (__, topicListeners) -> {
            topicListeners.listeners.remove(listener);
            // Nobody is left to be told about the policies of this topic, nor to be told that they are gone.
            return topicListeners.listeners.isEmpty() ? null : topicListeners;
        });
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (cancelLocalStoreListener != null) {
                cancelLocalStoreListener.run();
            }
            if (cancelConfigurationStoreListener != null) {
                cancelConfigurationStoreListener.run();
            }
            if (cancelLocalSessionListener != null) {
                cancelLocalSessionListener.run();
            }
            if (cancelConfigurationSessionListener != null) {
                cancelConfigurationSessionListener.run();
            }
            listeners.clear();
            if (localPoliciesCache != null) {
                localPoliciesCache.invalidateAll();
            }
            if (globalPoliciesCache != null) {
                globalPoliciesCache.invalidateAll();
            }
        }
    }

    private MetadataCache<TopicPolicies> cache(boolean isGlobalPolicy) {
        return isGlobalPolicy ? globalPoliciesCache : localPoliciesCache;
    }

    private CompletableFuture<Void> deleteIfExists(MetadataCache<TopicPolicies> cache, String path) {
        return cache.delete(path).handle((__, error) -> {
            cache.invalidate(path);
            if (error == null || FutureUtil.unwrapCompletionException(error) instanceof NotFoundException) {
                return null;
            }
            throw FutureUtil.wrapToCompletionException(error);
        });
    }

    private static TopicPolicies updatePolicies(Optional<TopicPolicies> currentPolicies,
                                                boolean isGlobalPolicy,
                                                Consumer<TopicPolicies> policyUpdater) {
        TopicPolicies policies = currentPolicies.map(TopicPolicies::clone).orElseGet(TopicPolicies::new);
        policies.setIsGlobal(isGlobalPolicy);
        policyUpdater.accept(policies);
        return policies;
    }

    private void handleNotification(Notification notification, boolean isGlobalPolicy) {
        if (closed.get()
                || (notification.getType() != NotificationType.Created
                && notification.getType() != NotificationType.Modified
                && notification.getType() != NotificationType.Deleted)) {
            return;
        }
        String path = notification.getPath();
        String root = isGlobalPolicy ? GLOBAL_POLICIES_ROOT : LOCAL_POLICIES_ROOT;
        Optional<TopicName> topicName = topicNameFromPath(root, path);
        if (topicName.isEmpty()) {
            return;
        }
        MetadataCache<TopicPolicies> cache = cache(isGlobalPolicy);
        if (notification.getType() == NotificationType.Deleted) {
            notifyListeners(topicName.get(), isGlobalPolicy, null);
            return;
        }
        // The metadata store refreshes its caches when it dispatches the notification, before this listener runs, so
        // the read below already observes the new value. Invalidating the path first would only widen the window in
        // which the cache holds no value at all, during which a cache-only read sees the policy as absent rather
        // than as its previous value, and would add a second, redundant read of the store (issue #26453).
        cache.get(path).whenComplete((policies, error) -> {
            if (error != null) {
                log.warn()
                        .attr("path", path)
                        .exception(error)
                        .log("Failed to refresh topic policies after metadata notification");
                return;
            }
            notifyListeners(topicName.get(), isGlobalPolicy,
                    policies.map(policy -> cloneWithScope(policy, isGlobalPolicy)).orElse(null));
        });
    }

    private void notifyListeners(TopicName topicName, boolean isGlobalPolicy, @Nullable TopicPolicies policies) {
        TopicListeners topicListeners = listeners.get(topicName);
        if (topicListeners == null) {
            return;
        }
        topicListeners.setDelivered(isGlobalPolicy, policies != null);
        for (TopicPolicyListener listener : topicListeners.listeners) {
            try {
                listener.onUpdate(policies == null ? null : policies.clone());
            } catch (Throwable error) {
                log.error().attr("topic", topicName).exception(error).log("Call topic policy listener error");
            }
        }
    }

    private void markDelivered(TopicName topicName, boolean isGlobalPolicy) {
        TopicListeners topicListeners = listeners.get(topicName);
        if (topicListeners != null) {
            topicListeners.setDelivered(isGlobalPolicy, true);
        }
    }

    private boolean isDelivered(TopicName topicName, boolean isGlobalPolicy) {
        TopicListeners topicListeners = listeners.get(topicName);
        return topicListeners != null && topicListeners.isDelivered(isGlobalPolicy);
    }

    private static TopicName normalizeTopicName(TopicName topicName) {
        return TopicName.get(topicName.getPartitionedTopicName());
    }

    /**
     * The listeners of one topic, and whether they were last told that the topic has policies, per scope.
     *
     * <p>The metadata store discards its caches when the session is re-established, so they cannot tell a policy that
     * was deleted while the session was lost from a topic that never had one in that scope; this can. It is kept
     * with the listeners, so that it goes away with the last one.
     */
    private static final class TopicListeners {
        private final List<TopicPolicyListener> listeners = new CopyOnWriteArrayList<>();
        private volatile boolean localDelivered;
        private volatile boolean globalDelivered;

        boolean isDelivered(boolean isGlobalPolicy) {
            return isGlobalPolicy ? globalDelivered : localDelivered;
        }

        void setDelivered(boolean isGlobalPolicy, boolean delivered) {
            if (isGlobalPolicy) {
                globalDelivered = delivered;
            } else {
                localDelivered = delivered;
            }
        }
    }

    private static TopicPolicies cloneWithScope(TopicPolicies policies, boolean isGlobalPolicy) {
        TopicPolicies cloned = policies.clone();
        cloned.setIsGlobal(isGlobalPolicy);
        return cloned;
    }

    @VisibleForTesting
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesDirectFromStore(TopicName topicName,
                                                                                      boolean isGlobal) {
        String path = pathFor(topicName, isGlobal);
        MetadataCache<TopicPolicies> c = cache(isGlobal);
        c.invalidate(path);
        return c.get(path).thenApply(opt -> opt.map(p -> cloneWithScope(p, isGlobal)));
    }

    @VisibleForTesting
    static String pathFor(TopicName topicName, boolean isGlobalPolicy) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        return (isGlobalPolicy ? GLOBAL_POLICIES_ROOT : LOCAL_POLICIES_ROOT)
                + "/" + partitionedTopicName.getTenant()
                + "/" + partitionedTopicName.getNamespacePortion()
                + "/" + partitionedTopicName.getDomain()
                + "/" + partitionedTopicName.getEncodedLocalName();
    }

    @VisibleForTesting
    private static Optional<TopicName> topicNameFromPath(String root, String path) {
        if (!path.startsWith(root + "/")) {
            return Optional.empty();
        }
        String[] parts = path.substring(root.length() + 1).split("/", 4);
        if (parts.length != 4) {
            return Optional.empty();
        }
        return Optional.of(TopicName.get(parts[2], parts[0], parts[1], Codec.decode(parts[3])));
    }
}
