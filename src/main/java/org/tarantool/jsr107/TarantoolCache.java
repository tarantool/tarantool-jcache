/**
 * Copyright 2011-2013 Terracotta, Inc.
 * Copyright 2011-2013 Oracle America Incorporated
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tarantool.jsr107;

import org.tarantool.cache.TarantoolCursor;
import org.tarantool.cache.TarantoolSession;
import org.tarantool.cache.TarantoolSpace;
import org.tarantool.cache.TarantoolTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tarantool.jsr107.event.CacheEntryListenerRegistration;
import org.tarantool.jsr107.event.CacheEventDispatcher;
import org.tarantool.jsr107.management.MBeanServerRegistrationUtility;
import org.tarantool.jsr107.management.TarantoolCacheMXBean;
import org.tarantool.jsr107.management.TarantoolCacheStatisticsMXBean;
import org.tarantool.jsr107.processor.ProcessorEntry;
import org.tarantool.jsr107.processor.ProcessorResult;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.integration.CompletionListener;
import javax.cache.management.CacheMXBean;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.tarantool.jsr107.management.MBeanServerRegistrationUtility.ObjectNameType.Configuration;
import static org.tarantool.jsr107.management.MBeanServerRegistrationUtility.ObjectNameType.Statistics;

/**
 * The implementation of the {@link Cache}.
 * Each TarantoolCache associated with Tarantool's {space_object},
 * (See 'box.space.{space_object_name}' in Tarantool's help)
 * So every TarantoolCache, successfully created from within {@link TarantoolCacheManager},
 * means {space_object} with given name exists within corresponding Tarantool instance.
 * <p>
 * For every newly created {space_object} key field and hash index for this key field are built.
 * <p>
 * But when {@link TarantoolCache} or {@link TarantoolCacheManager} are about to be closed,
 * we shouldn't drop {space_object} in order to prevent performance degradation.
 * We should provide cache element eviction/expiration policy instead.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values*
 * @author Brian Oliver
 * @author Greg Luck
 * @author Yannis Cosmadopoulos
 * @author Evgeniy Zaikin
 */
public final class TarantoolCache<K, V> implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(TarantoolCache.class);

    /**
     * The name of the {@link Cache} as used with in the scope of the
     * Cache Manager.
     */
    private final String cacheName;

    /**
     * The {@link TarantoolSpace} is Space representation of this cache
     */
    private final TarantoolSpace<K, V> space;

    /**
     * The {@link TarantoolCursor} represents abstract cursor for Tarantool operations on space
     */
    private final TarantoolCursor<K, V> cursor;

    /**
     * The {@link CacheManager} that created this implementation
     */
    private final TarantoolCacheManager cacheManager;

    /**
     * The {@link Configuration} for the {@link Cache}.
     */
    private final MutableConfiguration<K, V> configuration;

    /**
     * The internal {@link Configuration} for the {@link Cache}.
     */
    private final CompleteConfiguration<K, V> internalConfiguration;

    /**
     * The {@link CacheLoader} for the {@link Cache}.
     */
    private CacheLoader<K, V> cacheLoader;

    /**
     * The {@link CacheWriter} for the {@link Cache}.
     */
    private CacheWriter<K, V> cacheWriter;

    /**
     * The {@link ExpiryPolicy} for the {@link Cache}.
     */
    private final ExpiryPolicy expiryPolicy;

    /**
     * The List of {@link CacheEntryListenerRegistration} for the
     * {@link Cache}.
     */
    private final CopyOnWriteArrayList<CacheEntryListenerRegistration<K,
            V>> listenerRegistrations;

    /**
     * Event dispatcher is used for dispatching Events to all the Listeners
     */
    private final CacheEventDispatcher<K, V> eventDispatcher;

    /**
     * The open/closed state of the Cache.
     */
    private volatile boolean isClosed;

    private final TarantoolCacheMXBean<K, V> cacheMXBean;
    private final TarantoolCacheStatisticsMXBean statistics;

    /**
     * An {@link ExecutorService} for the purposes of performing asynchronous
     * background work.
     */
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    private static final long EXECUTOR_WAIT_TIMEOUT = 10;

    /**
     * Constructs a cache.
     *
     * @param cacheManager  the CacheManager that's creating the TarantoolCache
     * @param cacheName     the name of the Cache
     * @param classLoader   the ClassLoader the TarantoolCache will use for loading classes
     * @param session       the {@link TarantoolSession} associated with cacheManager
     * @param configuration the Configuration of the Cache
     */
    TarantoolCache(TarantoolCacheManager cacheManager,
                   String cacheName,
                   ClassLoader classLoader,
                   TarantoolSession session,
                   Configuration<K, V> configuration) {

        if (!configuration.isStoreByValue()) {
            throw new UnsupportedOperationException("StoreByReference mode is not supported");
        }

        this.cacheManager = cacheManager;
        this.cacheName = cacheName;
        this.space = new TarantoolSpace<K, V>(session, cacheName);

        //we make a copy of the configuration here so that the provided one
        //may be changed and or used independently for other caches.  we do this
        //as we don't know if the provided configuration is mutable
        if (configuration instanceof CompleteConfiguration) {
            //support use of CompleteConfiguration
            this.configuration = new MutableConfiguration<>((MutableConfiguration<K, V>) configuration);
        } else {
            //support use of Basic Configuration
            MutableConfiguration<K, V> mutableConfiguration = new MutableConfiguration<>();
            mutableConfiguration.setStoreByValue(configuration.isStoreByValue());
            mutableConfiguration.setTypes(configuration.getKeyType(), configuration.getValueType());
            this.configuration = mutableConfiguration;
        }

        if (this.configuration.getCacheLoaderFactory() != null) {
            cacheLoader = (CacheLoader<K, V>) this.configuration.getCacheLoaderFactory().create();
        }
        @SuppressWarnings("unchecked")
        Factory<CacheWriter<K, V>> cacheWriterFactory =
                (Factory<CacheWriter<K, V>>) (Object) this.configuration.getCacheWriterFactory();
        if (cacheWriterFactory != null) {
            cacheWriter = cacheWriterFactory.create();
        }

        if (session.getSessionConfiguration() != null) {
            internalConfiguration = getConfiguration(
                    session.getSessionConfiguration().getCacheConfiguration(cacheName),
                    session.getSessionConfiguration().getDefaultCacheConfiguration());
        } else {
            internalConfiguration = null;
        }

        if (internalConfiguration != null) {
            expiryPolicy = internalConfiguration.getExpiryPolicyFactory().create();
        } else {
            expiryPolicy = this.configuration.getExpiryPolicyFactory().create();
        }

        //establish all of the listeners
        listenerRegistrations = new CopyOnWriteArrayList<CacheEntryListenerRegistration<K, V>>();
        for (CacheEntryListenerConfiguration<K, V> listenerConfiguration :
                this.configuration.getCacheEntryListenerConfigurations()) {
            createAndAddListener(listenerConfiguration);
        }
        eventDispatcher = new CacheEventDispatcher<K, V>(this, listenerRegistrations);

        cursor = new TarantoolCursor<K, V>(space, expiryPolicy, eventDispatcher);

        cacheMXBean = new TarantoolCacheMXBean<>(this);
        statistics = new TarantoolCacheStatisticsMXBean();
        //It's important that we set the status BEFORE we let management,
        //statistics and listeners know about the cache.
        isClosed = false;

        if (this.configuration.isManagementEnabled()) {
            MBeanServerRegistrationUtility.registerCacheObject(this, Configuration);
        }

        if (this.configuration.isStatisticsEnabled()) {
            MBeanServerRegistrationUtility.registerCacheObject(this, Statistics);
        }

        if (internalConfiguration != null) {
            if (internalConfiguration.isManagementEnabled() && !this.configuration.isManagementEnabled()) {
                MBeanServerRegistrationUtility.registerCacheObject(this, Configuration);
            }
            if (internalConfiguration.isStatisticsEnabled() && !this.configuration.isStatisticsEnabled()) {
                MBeanServerRegistrationUtility.registerCacheObject(this, Statistics);
            }
        }
    }

    private void createAndAddListener(CacheEntryListenerConfiguration<K, V> listenerConfiguration) {
        CacheEntryListenerRegistration<K, V> registration = new
                CacheEntryListenerRegistration<K, V>(listenerConfiguration);
        listenerRegistrations.add(registration);
    }

    private void removeListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration == null) {
            throw new NullPointerException("CacheEntryListenerConfiguration can't be null");
        }
        for (CacheEntryListenerRegistration<K, V> listenerRegistration : listenerRegistrations) {
            if (cacheEntryListenerConfiguration.equals(listenerRegistration.getConfiguration())) {
                listenerRegistrations.remove(listenerRegistration);
                configuration.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
            }
        }
    }

    /**
     * Requests a {@link Runnable} to be performed.
     *
     * @param task the {@link Runnable} to be performed
     */
    protected void submit(Runnable task) {
        executorService.submit(task);
    }

    /**
     * Selects first valid (non null) Configuration {@link Configuration} from
     * given {@link Configuration}s
     *
     * @param Configuration<?,?> all available configurations
     * @param <K>                type of keys
     * @param <V>                type of values
     * @return Configuration<K                                                               ,                                                               V> selected from all available configurations
     */
    @SuppressWarnings("unchecked")
    protected <C extends Configuration<K, V>> C getConfiguration(Configuration<?, ?>... configurations) {
        for (Configuration<?, ?> configuration : configurations) {
            if (configuration != null) {
                return (C) configuration;
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return cacheName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() {
        if (!isClosed()) {
            //ensure that any further access to this Cache will raise an
            // IllegalStateException
            isClosed = true;

            //ensure that the cache may no longer be accessed via the CacheManager
            cacheManager.releaseCache(cacheName);

            //disable statistics and management
            setStatisticsEnabled(false);
            setManagementEnabled(false);

            //close the configured CacheLoader
            if (cacheLoader instanceof Closeable) {
                try {
                    ((Closeable) cacheLoader).close();
                } catch (IOException e) {
                    log.error("Exception occurred during closing CacheLoader", e);
                }
            }

            //close the configured CacheWriter
            if (cacheWriter instanceof Closeable) {
                try {
                    ((Closeable) cacheWriter).close();
                } catch (IOException e) {
                    log.error("Exception occurred during closing CacheWriter", e);
                }
            }

            //close the configured ExpiryPolicy
            if (expiryPolicy instanceof Closeable) {
                try {
                    ((Closeable) expiryPolicy).close();
                } catch (IOException e) {
                    log.error("Exception occurred during closing ExpiryPolicy", e);
                }
            }

            //close the configured CacheEntryListeners
            for (CacheEntryListenerRegistration<K, V> registration : listenerRegistrations) {
                if (registration.getCacheEntryListener() instanceof Closeable) {
                    try {
                        ((Closeable) registration.getCacheEntryListener()).close();
                    } catch (IOException e) {
                        log.error("Exception occurred during closing listener " +
                                registration.getCacheEntryListener().getClass(), e);
                    }
                }
            }

            //attempt to shutdown (and wait for the cache to shutdown)
            executorService.shutdown();
            try {
                // Wait a while for existing tasks to terminate
                if (!executorService.awaitTermination(EXECUTOR_WAIT_TIMEOUT, TimeUnit.SECONDS)) {
                    executorService.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!executorService.awaitTermination(EXECUTOR_WAIT_TIMEOUT, TimeUnit.SECONDS)) {
                        log.error("executorService did not terminate");
                    }
                }
            } catch (InterruptedException e) {
                // (Re-)Cancel if current thread also interrupted
                executorService.shutdownNow();
                throw new CacheException(e);
            }

            //FIXME: remove this call
            space.truncate();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(configuration)) {
            return clazz.cast(configuration);
        }
        throw new IllegalArgumentException("The configuration class " + clazz +
                " is not supported by this implementation");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }

        return getValue(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        ensureOpen();
        // will throw NPE if keys=null
        HashMap<K, V> map = new HashMap<>(keys.size());

        for (K key : keys) {
            if (key == null) {
                throw new NullPointerException("keys contains a null");
            }
            V value = getValue(key);
            if (value != null) {
                map.put(key, value);
            }
        }

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException();
        }

        long now = System.currentTimeMillis();
        boolean result;
        try {
            result = cursor.locate(key) && !cursor.isExpiredAt(now);
        } finally {
            cursor.close();
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadAll(final Set<? extends K> keys,
                        final boolean replaceExistingValues,
                        final CompletionListener completionListener) {
        ensureOpen();
        if (keys == null) {
            throw new NullPointerException("keys");
        }

        if (cacheLoader == null) {
            if (completionListener != null) {
                completionListener.onCompletion();
            }
        } else {
            for (K key : keys) {
                if (key == null) {
                    throw new NullPointerException("keys contains a null");
                }
            }

            submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        ArrayList<K> keysToLoad = new ArrayList<K>();
                        for (K key : keys) {
                            if (replaceExistingValues || !containsKey(key)) {
                                keysToLoad.add(key);
                            }
                        }

                        Map<? extends K, ? extends V> loaded;
                        try {
                            loaded = cacheLoader.loadAll(keysToLoad);
                        } catch (Exception e) {
                            if (!(e instanceof CacheLoaderException)) {
                                throw new CacheLoaderException("Exception in CacheLoader", e);
                            } else {
                                throw e;
                            }
                        }

                        for (K key : keysToLoad) {
                            if (loaded.get(key) == null) {
                                loaded.remove(key);
                            }
                        }

                        putAll(loaded, replaceExistingValues, false);

                        if (completionListener != null) {
                            completionListener.onCompletion();
                        }
                    } catch (Exception e) {
                        if (completionListener != null) {
                            completionListener.onException(e);
                        }
                    }
                }
            });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(K key, V value) {
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        int putCount = 0;
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        checkTypesAgainstConfiguredTypes(key, value);

        long now = System.currentTimeMillis();
        if (cursor.locate(key)) {
            V oldValue = cursor.getValue();
            cursor.update(value, now);
            writeCacheEntry(key, value, oldValue);
            putCount++;
        } else if (cursor.insert(key, value, now)) {
            writeCacheEntry(key, value, null);
            putCount++;
        }
        if (statisticsEnabled() && putCount > 0) {
            statistics.increaseCachePuts(putCount);
            statistics.addPutTimeNano(System.nanoTime() - start);
        }
    }

    private void checkTypesAgainstConfiguredTypes(K key, V value) throws ClassCastException {
        Class<K> keyType = configuration.getKeyType();
        Class<V> valueType = configuration.getValueType();
        if (Object.class != keyType) {
            //means type checks required
            if (!keyType.isAssignableFrom(key.getClass())) {
                throw new ClassCastException("Key " + key + "is not assignable to " + keyType);
            }
        }
        if (Object.class != valueType) {
            //means type checks required
            if (!valueType.isAssignableFrom(value.getClass())) {
                throw new ClassCastException("Value " + value + "is not assignable to " + valueType);
            }
        }
    }

    @Override
    public V getAndPut(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        long start = statisticsEnabled() ? System.nanoTime() : 0;
        long now = System.currentTimeMillis();

        V result = null;
        int putCount = 0;
        try {

            if (cursor.locate(key)) {
                if (!cursor.isExpiredAt(now)) {
                    V oldValue = cursor.getValue();
                    result = oldValue;
                    cursor.update(value, now);
                    writeCacheEntry(key, value, oldValue);

                } else {

                    cursor.update(value, now);
                    writeCacheEntry(key, value, null);
                }
                putCount++;

            } else {

                if (cursor.insert(key, value, now)) {
                    writeCacheEntry(key, value, null);
                    putCount++;
                }
            }

        } finally {
            cursor.close();
        }
        if (statisticsEnabled()) {
            if (result == null) {
                statistics.increaseCacheMisses(1);
            } else {
                statistics.increaseCacheHits(1);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);

            if (putCount > 0) {
                statistics.increaseCachePuts(putCount);
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        putAll(map, true, true);
    }

    /**
     * A implementation of PutAll that allows optional replacement of existing
     * values and optionally writing values when Write Through is configured.
     *
     * @param map                   the Map of entries to put
     * @param replaceExistingValues should existing values be replaced by those in
     *                              the map?
     * @param useWriteThrough       should write-through be used if it is configured
     */
    public void putAll(Map<? extends K, ? extends V> map,
                       final boolean replaceExistingValues,
                       boolean useWriteThrough) {
        ensureOpen();
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        long now = System.currentTimeMillis();
        int putCount = 0;

        CacheWriterException exception = null;

        try {
            boolean isWriteThrough = configuration.isWriteThrough() && cacheWriter !=
                    null && useWriteThrough;

            ArrayList<Cache.Entry<? extends K, ? extends V>> entriesToWrite = new
                    ArrayList<>();
            HashSet<K> keysToPut = new HashSet<>();
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();

                if (key == null) {
                    throw new NullPointerException("key");
                }

                if (value == null) {
                    throw new NullPointerException("key " + key + " has a null value");
                }

                keysToPut.add(key);

                if (isWriteThrough) {
                    entriesToWrite.add(new CacheEntry<>(key, value));
                }
            }

            //write the entries
            if (isWriteThrough) {
                try {
                    cacheWriter.writeAll(entriesToWrite);
                } catch (CacheWriterException e) {
                    exception = e;
                } catch (Exception e) {
                    exception = new CacheWriterException("Exception during write", e);
                }

                for (Entry<?, ?> entry : entriesToWrite) {
                    keysToPut.remove(entry.getKey());
                }
            }

            //perform the put
            for (K key : keysToPut) {
                V value = map.get(key);

                if (replaceExistingValues) {
                    // do not count loadAll calls as puts. useWriteThrough is false when
                    // called from loadAll.
                    if (cursor.locate(key)) {
                        cursor.update(value, now);
                        if (useWriteThrough) {
                            putCount++;
                        }
                    } else if (cursor.insert(key, value, now)) {
                        if (useWriteThrough) {
                            putCount++;
                        }
                    }
                } else {
                    // this method called from loadAll when useWriteThrough is false. do
                    // not count loads as puts per statistics
                    // table in specification.
                    if (cursor.insert(key, value, now)) {
                        if (useWriteThrough) {
                            putCount++;
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }

        if (statisticsEnabled() && putCount > 0) {
            statistics.increaseCachePuts(putCount);
            statistics.addPutTimeNano(System.nanoTime() - start);
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean putIfAbsent(K key, V value) {
        ensureOpen();
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        checkTypesAgainstConfiguredTypes(key, value);

        long start = statisticsEnabled() ? System.nanoTime() : 0;

        long now = System.currentTimeMillis();

        boolean result = false;
        if (!cursor.locate(key) && cursor.insert(key, value, now)) {
            writeCacheEntry(key, value, null);
            result = true;
        }

        if (statisticsEnabled()) {
            if (result) {
                //this means that there was no key in the Cache and the put succeeded
                statistics.increaseCachePuts(1);
                statistics.increaseCacheMisses(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                //this means that there was a key in the Cache and the put did not succeed
                statistics.increaseCacheHits(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        long now = System.currentTimeMillis();
        deleteCacheEntry(key);
        boolean result = cursor.delete(key) && !cursor.isExpiredAt(now);
        if (result && statisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNano(System.nanoTime() - start);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(K key, V oldValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (oldValue == null) {
            throw new NullPointerException("null oldValue specified for key " + key);
        }

        long now = System.currentTimeMillis();
        long hitCount = 0;

        long start = statisticsEnabled() ? System.nanoTime() : 0;
        boolean result;
        if (!cursor.locate(key)) {
            result = false;
        } else if (cursor.isExpiredAt(now)) {
            result = false;
        } else {
            hitCount++;

            if (oldValue.equals(cursor.getValue())) {
                deleteCacheEntry(key);
                result = cursor.delete();
            } else {
                cursor.access(now);
                result = false;
            }
        }
        if (statisticsEnabled()) {
            if (result) {
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNano(System.nanoTime() - start);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V getAndRemove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }

        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        deleteCacheEntry(key);
        V value = null;
        if (cursor.delete(key)) {
            value = cursor.isExpiredAt(now) ? null : cursor.getValue();
        }
        if (statisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (value != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (newValue == null) {
            throw new NullPointerException("null newValue specified for key " + key);
        }
        if (oldValue == null) {
            throw new NullPointerException("null oldValue specified for key " + key);
        }

        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        long hitCount = 0;

        boolean result = false;
        try {
            if (cursor.locate(key) && !cursor.isExpiredAt(now)) {
                hitCount++;

                if (oldValue.equals(cursor.getValue())) {

                    cursor.update(newValue, now);
                    writeCacheEntry(key, newValue, oldValue);
                    result = true;

                } else {

                    cursor.access(now);
                    result = false;
                }
            }
        } catch (Exception e) {
            throw e;
        }
        if (statisticsEnabled()) {
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean replace(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        boolean result = false;
        try {
            if (cursor.locate(key) && !cursor.isExpiredAt(now)) {
                V oldValue = cursor.getValue();
                cursor.update(value, now);
                writeCacheEntry(key, value, oldValue);
                result = true;
            }
        } finally {
            cursor.close();
        }
        if (statisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.increaseCacheHits(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V getAndReplace(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        V result = null;
        try {
            if (cursor.locate(key) && !cursor.isExpiredAt(now)) {
                V oldValue = cursor.getValue();
                result = oldValue;
                cursor.update(value, now);
                writeCacheEntry(key, value, oldValue);
            }
        } finally {
            cursor.close();
        }
        if (statisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAll(Set<? extends K> keys) {
        ensureOpen();
        CacheException exception = null;
        if (keys.size() > 0) {
            boolean isWriteThrough = configuration.isWriteThrough() && cacheWriter != null;
            HashSet<K> deletedKeys = new HashSet<>();
            for (K key : keys) {
                if (key == null) {
                    throw new NullPointerException("keys contains a null");
                }
            }
            //call write-through on deleted entries
            if (isWriteThrough) {
                HashSet<K> cacheWriterKeys = new HashSet<>(keys);
                try {
                    cacheWriter.deleteAll(cacheWriterKeys);
                } catch (CacheWriterException e) {
                    exception = e;
                } catch (Exception e) {
                    exception = new CacheWriterException("Exception during write", e);
                }

                //At this point, cacheWriterKeys will contain only those that were _not_ written
                //Now delete only those that the writer deleted
                for (K key : keys) {
                    //only delete those keys that the writer deleted. per CacheWriter spec.
                    if (!cacheWriterKeys.contains(key)) {
                        if (cursor.delete(key)) {
                            deletedKeys.add(key);
                        }
                    }
                }
            } else {

                for (K key : keys) {
                    if (cursor.delete(key)) {
                        deletedKeys.add(key);
                    }
                }
            }

            //Update stats
            if (statisticsEnabled()) {
                statistics.increaseCacheRemovals(deletedKeys.size());
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAll() {
        ensureOpen();
        int cacheRemovals = 0;
        long now = System.currentTimeMillis();
        CacheException exception = null;
        HashSet<K> allExpiredKeys = new HashSet<>();
        HashSet<K> allNonExpiredKeys = new HashSet<>();
        HashSet<K> keysToDelete = new HashSet<>();

        try {
            boolean isWriteThrough = configuration.isWriteThrough() && cacheWriter != null;
            for (TarantoolTuple<K, V> tuple : space) {
                if (tuple.isExpiredAt(now)) {
                    allExpiredKeys.add(tuple.getKey());
                } else {
                    allNonExpiredKeys.add(tuple.getKey());
                }
                if (isWriteThrough) {
                    keysToDelete.add(tuple.getKey());
                }
            }

            //delete the entries (when there are some)
            if (isWriteThrough && keysToDelete.size() > 0) {
                try {
                    cacheWriter.deleteAll(keysToDelete);
                } catch (CacheWriterException e) {
                    exception = e;
                } catch (Exception e) {
                    exception = new CacheWriterException("Exception during write", e);
                }
            }

            //remove the deleted keys that were successfully deleted from the set (only non-expired)
            for (K key : allNonExpiredKeys) {
                if (!keysToDelete.contains(key)) {
                    if (cursor.delete(key)) {
                        cacheRemovals++;
                    }
                }
            }

            //remove the deleted keys that were successfully deleted from the set (only expired)
            for (K key : allExpiredKeys) {
                if (!keysToDelete.contains(key)) {
                    cursor.delete(key);
                }
            }
        } catch (Exception e) {
            throw e;
        }

        if (statisticsEnabled()) {
            statistics.increaseCacheRemovals(cacheRemovals);
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        ensureOpen();
        space.truncate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T invoke(K key, javax.cache.processor.EntryProcessor<K, V,
            T> entryProcessor, Object... arguments) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException();
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }

        long start = statisticsEnabled() ? System.nanoTime() : 0;


        T result;
        V newValue;
        V oldValue;
        try {
            long now = System.currentTimeMillis();

            cursor.locate(key);

            if (statisticsEnabled()) {
                if (!cursor.isLocated()) {
                    statistics.increaseCacheMisses(1);
                } else {
                    statistics.increaseCacheHits(1);
                }

                statistics.addGetTimeNano(System.nanoTime() - start);
            }

            //restart start as fetch finished
            start = statisticsEnabled() ? System.nanoTime() : 0;

            ProcessorEntry<K, V> entry = new ProcessorEntry<>(key,
                    cursor, now, configuration.isReadThrough() ? cacheLoader : null);
            try {
                result = entryProcessor.process(entry, arguments);
            } catch (CacheException e) {
                throw e;
            } catch (Exception e) {
                throw new EntryProcessorException(e);
            }

            switch (entry.getOperation()) {
                case NONE:
                    break;

                case ACCESS:
                    cursor.access(now);
                    break;

                case CREATE:
                    newValue = entry.getValue();
                    if (cursor.insert(key, newValue, now)) {
                        writeCacheEntry(key, newValue, null);
                        if (statisticsEnabled()) {
                            statistics.increaseCachePuts(1);
                            statistics.addPutTimeNano(System.nanoTime() - start);
                        }
                    }

                    break;

                case LOAD:
                    newValue = entry.getValue();
                    cursor.insert(key, newValue, now);

                    break;

                case UPDATE:
                    newValue = entry.getValue();
                    oldValue = cursor.getValue();
                    cursor.update(newValue, now);
                    writeCacheEntry(key, newValue, oldValue);

                    if (statisticsEnabled()) {
                        statistics.increaseCachePuts(1);
                        statistics.addPutTimeNano(System.nanoTime() - start);
                    }

                    break;

                case REMOVE:
                    deleteCacheEntry(key);
                    cursor.delete(key);

                    if (statisticsEnabled()) {
                        statistics.increaseCacheRemovals(1);
                        statistics.addRemoveTimeNano(System.nanoTime() - start);
                    }

                    break;

                default:
                    break;
            }


        } finally {
            cursor.close();
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
                                                         EntryProcessor<K, V, T> entryProcessor,
                                                         Object... arguments) {
        ensureOpen();
        if (keys == null) {
            throw new NullPointerException();
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }

        HashMap<K, EntryProcessorResult<T>> map = new HashMap<>();
        for (K key : keys) {
            ProcessorResult<T> result;
            try {
                T t = invoke(key, entryProcessor, arguments);
                result = t == null ? null : new ProcessorResult<>(t);
            } catch (Exception e) {
                result = new ProcessorResult<>(e);
            }
            if (result != null) {
                map.put(key, result);
            }
        }

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        long now = System.currentTimeMillis();
        return new Iterator<Entry<K, V>>() {
            private Iterator<TarantoolTuple<K, V>> iterator = cursor.iterator(now);

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<K, V> next() {
                long start = statisticsEnabled() ? System.nanoTime() : 0;
                TarantoolTuple<K, V> result = iterator.next();
                if (statisticsEnabled()) {
                    statistics.increaseCacheHits(1);
                    statistics.addGetTimeNano(System.nanoTime() - start);
                }
                return new CacheEntry<K, V>(result.getKey(), result.getValue());
            }

            @Override
            public void remove() {
                if (cursor.isLocated()) {
                    TarantoolCache.this.remove(cursor.getKey());
                } else {
                    throw new IllegalStateException("Must progress to the next entry to remove");
                }
            }
        };
    }

    /**
     * @return the management bean
     */
    public CacheMXBean getCacheMXBean() {
        return cacheMXBean;
    }


    /**
     * @return the management bean
     */
    public CacheStatisticsMXBean getCacheStatisticsMXBean() {
        return statistics;
    }


    /**
     * Sets statistics
     */
    public void setStatisticsEnabled(boolean enabled) {
        if (enabled) {
            MBeanServerRegistrationUtility.registerCacheObject(this, Statistics);
        } else {
            MBeanServerRegistrationUtility.unregisterCacheObject(this, Statistics);
        }
        configuration.setStatisticsEnabled(enabled);
    }


    /**
     * Sets management
     *
     * @param enabled true if management should be enabled
     */
    public void setManagementEnabled(boolean enabled) {
        if (enabled) {
            MBeanServerRegistrationUtility.registerCacheObject(this, Configuration);
        } else {
            MBeanServerRegistrationUtility.unregisterCacheObject(this, Configuration);
        }
        configuration.setManagementEnabled(enabled);
    }

    private void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. " +
                    "The cache closed");
        }
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> cls) {
        if (cls.isAssignableFrom(((Object) this).getClass())) {
            return cls.cast(this);
        }

        throw new IllegalArgumentException("Unwrapping to " + cls + " is not " +
                "supported by this implementation");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        configuration.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
        createAndAddListener(cacheEntryListenerConfiguration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K,
            V> cacheEntryListenerConfiguration) {
        removeListener(cacheEntryListenerConfiguration);
    }


    private boolean statisticsEnabled() {
        if (internalConfiguration != null) {
            return internalConfiguration.isStatisticsEnabled() || configuration.isStatisticsEnabled();
        }
        return configuration.isStatisticsEnabled();
    }

    /**
     * Writes the Cache Entry to the configured CacheWriter.  Does nothing if
     * write-through is not configured.
     * Also provides consistency here: if exception was thrown,
     * rolls back tarantool's tuple or just deletes it.
     *
     * @param key of the Cache Entry to write
     * @param value of the Cache Entry to write
     * @param oldValue previous value
     */
    private void writeCacheEntry(K key, V value, V oldValue) {
        if (configuration.isWriteThrough()) {
            long now = System.currentTimeMillis();
            try {
                /* Don't worry about the Heap: this entry is not persistent,
                 * and it is not associated with Tarantool's tuple.
                 * Garbage Collector will remove it sooner or later.
                 */
                cacheWriter.write(new CacheEntry<K, V>(key, value));
            } catch (Exception e) {
                /*
                 * Consistency. Update back to old value,
                 * or just delete tuple.
                 */
                if (oldValue != null) {
                    cursor.update(oldValue, now);
                } else {
                    cursor.delete();
                }
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter", e);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Deletes the Cache Entry using the configured CacheWriter.  Does nothing
     * if write-through is not configured.
     *
     * @param key
     */
    private void deleteCacheEntry(K key) {
        if (configuration.isWriteThrough()) {
            try {
                cacheWriter.delete(key);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter", e);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Gets the value for the specified key from the underlying cache, including
     * attempting to load it if a CacheLoader is configured (with read-through).
     * <p>
     * Any events that need to be raised are added to the specified dispatcher.
     * </p>
     *
     * @param key the key of the entry to get from the cache
     * @return the value loaded
     */
    private V getValue(K key) {
        long now = System.currentTimeMillis();
        long start = statisticsEnabled() ? System.nanoTime() : 0;

        V value = null;
        try {
            boolean isExpired = cursor.locate(key) && cursor.isExpiredAt(now);
            if (isExpired || !cursor.isLocated()) {
                if (statisticsEnabled()) {
                    statistics.increaseCacheMisses(1);
                }

                if (configuration.isReadThrough() && cacheLoader != null) {
                    try {
                        value = cacheLoader.load(key);
                    } catch (Exception e) {
                        if (!(e instanceof CacheLoaderException)) {
                            throw new CacheLoaderException("Exception in CacheLoader", e);
                        } else {
                            throw e;
                        }
                    }
                }

                if (value != null) {

                    /*
                     * Put loaded value to cache
                     * */
                    if (cursor.isLocated()) {
                        cursor.update(value, now);
                    } else {
                        cursor.insert(key, value, now);
                    }
                } else {

                    /*
                     * Don't put anything to cache (no Read-Through or no value loaded from сacheLoader).
                     * Process expired entry instead.
                     * */
                    if (isExpired) {
                        cursor.delete();
                    }
                }

            } else {
                value = cursor.fetch(now);

                if (statisticsEnabled()) {
                    statistics.increaseCacheHits(1);
                }
            }

        } finally {
            cursor.close();
            if (statisticsEnabled()) {
                statistics.addGetTimeNano(System.nanoTime() - start);
            }
        }
        return value;
    }

}
