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

import org.tarantool.cache.CacheStore;
import org.tarantool.cache.NativeCache;
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
import java.util.Collection;
import java.util.HashMap;
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
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Brian Oliver
 * @author Greg Luck
 * @author Yannis Cosmadopoulos
 * @author Evgeniy Zaikin
 */
public final class TarantoolCache<K, V> implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(TarantoolCache.class);
    private static final long EXECUTOR_WAIT_TIMEOUT = 10;

    /**
     * The name of the {@link Cache} as used with in the scope of the
     * Cache Manager.
     */
    private final String cacheName;

    /**
     * The {@link NativeCache} represents native cache,
     * that is associated with Tarantool's Space.
     * Uses write-through operations.
     */
    private final NativeCache<K, V> cache;

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
     * The {@link ExpiryPolicy} for the {@link Cache}.
     */
    private final ExpiryPolicy expiryPolicy;

    /**
     * The List of {@link CacheEntryListenerRegistration} for the
     * {@link Cache}.
     */
    private final CopyOnWriteArrayList<CacheEntryListenerRegistration<K,
            V>> listenerRegistrations;

    private final TarantoolCacheMXBean<K, V> cacheMXBean;
    private final TarantoolCacheStatisticsMXBean statistics;

    /**
     * An {@link ExecutorService} for the purposes of performing asynchronous
     * background work.
     */
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    /**
     * The {@link CacheLoader} for the {@link Cache}.
     */
    private CacheLoader<K, V> cacheLoader;

    /**
     * The {@link CacheWriter} for the {@link Cache}.
     */
    private CacheWriter<K, V> cacheWriter;

    /**
     * The {@link CacheWriterException} used to store and postpone an exception,
     * that can be thrown during writeAll/deleteAll operation.
     * Note: Don't forget to set null before calling writeAll/deleteAll.
     */
    private CacheWriterException cacheWriterException;

    /**
     * The open/closed state of the Cache.
     */
    private volatile boolean isClosed;

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

        final TarantoolSpace<K, V> space = new TarantoolSpace<K, V>(session, cacheName);
        // Create Expire Policy Converter to convert JSR-107 Duration type to time stamp (long)
        final ExpiryPolicyConverter expiryConverter = new ExpiryPolicyConverter(expiryPolicy);
        // Event dispatcher is used for dispatching Events to all the Listeners
        final CacheEventDispatcher<K, V> eventDispatcher = new CacheEventDispatcher<>(this, listenerRegistrations);
        // Creates CacheLoader and CacheWriter combined wrapper
        final CacheStore<K, V> cacheStore = new CacheLoaderWriter();

        cache = new NativeCache<K, V>(space, expiryConverter, eventDispatcher, cacheStore);

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
     * Selects first valid (non null) Configuration {@link Configuration} from
     * given {@link Configuration}s
     *
     * @param Configuration<?,?> all available configurations
     * @param <K>                type of keys
     * @param <V>                type of values
     * @return the requested implementation of {@link Configuration}
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
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        V value = cache.get(key);
        if (statisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (value != null) {
                statistics.increaseCacheHits(1);
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
    public Map<K, V> getAll(Set<? extends K> keys) {
        ensureOpen();
        // will throw NPE if keys=null
        HashMap<K, V> map = new HashMap<>(keys.size());
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        int hitsCount = 0;
        int missesCount = 0;
        for (K key : keys) {
            if (key == null) {
                throw new NullPointerException("keys contains a null");
            }
            V value = cache.get(key);
            if (value != null) {
                map.put(key, value);
                hitsCount++;
            } else {
                missesCount++;
            }
        }

        if (statisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            statistics.increaseCacheHits(hitsCount);
            statistics.increaseCacheMisses(missesCount);
        }

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(K key) {
        ensureOpen();
        return cache.containsKey(key);
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

            executorService.submit(new Runnable() {
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

                        cache.putAll(loaded, replaceExistingValues, false);

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
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        checkTypesAgainstConfiguredTypes(key, value);
        boolean result = cache.put(key, value);
        if (statisticsEnabled() && result) {
            statistics.increaseCachePuts(1);
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

        V result = cache.getAndPut(key, value);

        if (statisticsEnabled()) {
            if (result == null) {
                statistics.increaseCacheMisses(1);
            } else {
                statistics.increaseCacheHits(1);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);
            statistics.increaseCachePuts(1);
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        ensureOpen();
        long start = statisticsEnabled() ? System.nanoTime() : 0;
        boolean isWriteThrough = cacheWriter != null && configuration.isWriteThrough();

        cacheWriterException = null;
        int putCount = cache.putAll(map, true, isWriteThrough);

        if (statisticsEnabled() && putCount > 0) {
            statistics.increaseCachePuts(putCount);
            statistics.addPutTimeNano(System.nanoTime() - start);
        }

        if (cacheWriterException != null) {
            throw cacheWriterException;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean putIfAbsent(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        checkTypesAgainstConfiguredTypes(key, value);

        long start = statisticsEnabled() ? System.nanoTime() : 0;

        boolean result = cache.putIfAbsent(key, value);
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

        boolean result = cache.remove(key);
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

        long start = statisticsEnabled() ? System.nanoTime() : 0;
        boolean result = cache.remove(key, oldValue);
        if (statisticsEnabled()) {
            if (result) {
                statistics.increaseCacheRemovals(1);
                statistics.increaseCacheHits(1);
                statistics.addRemoveTimeNano(System.nanoTime() - start);
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

        long start = statisticsEnabled() ? System.nanoTime() : 0;

        V value = cache.getAndRemove(key);

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

        long start = statisticsEnabled() ? System.nanoTime() : 0;

        int status = cache.replace(key, oldValue, newValue);
        if (statisticsEnabled()) {
            if (status == 0) {
                statistics.increaseCacheMisses(1);
            } else {
                statistics.increaseCacheHits(1);
                statistics.addGetTimeNano(System.nanoTime() - start);
            }

            if (status > 0) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
        }
        return status > 0;
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

        long start = statisticsEnabled() ? System.nanoTime() : 0;
        boolean result = cache.replace(key, value);

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

        long start = statisticsEnabled() ? System.nanoTime() : 0;
        V result = cache.getAndReplace(key, value);

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
        cacheWriterException = null;
        boolean isWriteThrough = cacheWriter != null && configuration.isWriteThrough();
        int cacheRemovals = cache.removeAll(keys, isWriteThrough);

        if (statisticsEnabled() && cacheRemovals > 0) {
            statistics.increaseCacheRemovals(cacheRemovals);
        }

        if (cacheWriterException != null) {
            throw cacheWriterException;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeAll() {
        ensureOpen();
        cacheWriterException = null;
        boolean isWriteThrough = cacheWriter != null && configuration.isWriteThrough();
        int cacheRemovals = cache.removeAll(isWriteThrough);

        if (statisticsEnabled() && cacheRemovals > 0) {
            statistics.increaseCacheRemovals(cacheRemovals);
        }

        if (cacheWriterException != null) {
            throw cacheWriterException;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        ensureOpen();
        cache.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException();
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }

        long start = statisticsEnabled() ? System.nanoTime() : 0;


        T result;
        if (statisticsEnabled()) {
            if (cache.containsKey(key)) {
                statistics.increaseCacheHits(1);
            } else {
                statistics.increaseCacheMisses(1);
            }

            statistics.addGetTimeNano(System.nanoTime() - start);
        }

        //restart start as fetch finished
        start = statisticsEnabled() ? System.nanoTime() : 0;

        ProcessorEntry<K, V> entry = new ProcessorEntry<>(key, cache,
                configuration.isReadThrough() ? cacheLoader : null);
        try {
            result = entryProcessor.process(entry, arguments);
        } catch (CacheException e) {
            throw e;
        } catch (Exception e) {
            throw new EntryProcessorException(e);
        }
        cache.invoke(entry.getOperation(), key, entry.getValue());
        if (statisticsEnabled()) {
            switch (entry.getOperation()) {
                case CREATE:
                case UPDATE:
                    statistics.increaseCachePuts(1);
                    statistics.addPutTimeNano(System.nanoTime() - start);
                    break;

                case REMOVE:
                    statistics.increaseCacheRemovals(1);
                    statistics.addRemoveTimeNano(System.nanoTime() - start);
                    break;

                default:
                    break;
            }
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
        return new Iterator<Entry<K, V>>() {
            private Iterator<TarantoolTuple<K, V>> iterator = cache.iterator();

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
                long start = statisticsEnabled() ? System.nanoTime() : 0;
                iterator.remove();
                if (statisticsEnabled()) {
                    statistics.increaseCacheRemovals(1);
                    statistics.addRemoveTimeNano(System.nanoTime() - start);
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

    private final class CacheLoaderWriter implements CacheStore<K, V> {
        @Override
        public void write(K key, V value) {
            if (configuration.isWriteThrough()) {
                try {
                    /* Don't worry about the Heap: this entry is not persistent,
                     * and it is not associated with Tarantool's tuple.
                     * Garbage Collector will remove it sooner or later.
                     */
                    cacheWriter.write(new CacheEntry<K, V>(key, value));
                } catch (Exception e) {
                    if (!(e instanceof CacheWriterException)) {
                        throw new CacheWriterException("Exception in CacheWriter", e);
                    } else {
                        throw e;
                    }
                }
            }
        }

        @Override
        public void delete(K key) {
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

        @Override
        public V load(K key) {
            if (configuration.isReadThrough() && cacheLoader != null) {
                try {
                    return cacheLoader.load(key);
                } catch (Exception e) {
                    if (!(e instanceof CacheLoaderException)) {
                        throw new CacheLoaderException("Exception in CacheLoader", e);
                    } else {
                        throw e;
                    }
                }
            }
            return null;
        }

        @Override
        public void writeAll(Map<? extends K, ? extends V> map) {
            ArrayList<Cache.Entry<? extends K, ? extends V>> entriesToWrite = new
                    ArrayList<>();

            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                entriesToWrite.add(new CacheEntry<K, V>(key, value));
            }

            try {
                cacheWriter.writeAll(entriesToWrite);
            } catch (CacheWriterException e) {
                cacheWriterException = e;
            } catch (Exception e) {
                cacheWriterException = new CacheWriterException("Exception during write", e);
            }
            for (Cache.Entry<?, ?> entry : entriesToWrite) {
                map.remove(entry.getKey());
            }
        }

        @Override
        public void deleteAll(Collection<?> keys) {
            //delete the entries (when there are some)
            if (keys.size() > 0) {
                try {
                    cacheWriter.deleteAll(keys);
                } catch (CacheWriterException e) {
                    cacheWriterException = e;
                } catch (Exception e) {
                    cacheWriterException = new CacheWriterException("Exception during write", e);
                }
            }
        }
    }

}
