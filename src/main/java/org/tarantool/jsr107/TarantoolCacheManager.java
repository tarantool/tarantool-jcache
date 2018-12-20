/**
 *  Copyright 2011-2013 Terracotta, Inc.
 *  Copyright 2011-2013 Oracle America Incorporated
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.tarantool.jsr107;

import org.tarantool.cache.TarantoolSession;

import org.tarantool.jsr107.TarantoolCachingProvider;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The implementation of the {@link CacheManager}.
 * TarantoolCacheManager associated with one Tarantool instance.
 * 
 * @author Yannis Cosmadopoulos
 * @author Brian Oliver
 * @author Evgeniy Zaikin
 * @since 1.0
 */
public class TarantoolCacheManager implements CacheManager {

  private static final Logger LOGGER = Logger.getLogger("javax.cache");
  private final HashMap<String, TarantoolCache<?, ?>> caches = new HashMap<String, TarantoolCache<?, ?>>();

  private final TarantoolCachingProvider cachingProvider;
  private final TarantoolSession session;

  private final URI uri;
  private final WeakReference<ClassLoader> classLoaderReference;
  private final Properties properties;

  private volatile boolean isClosed;

  private void ensureOpen() {
      if (isClosed()) {
          throw new IllegalStateException("The cache is closed");
      }
  }

  /**
   * Constructs a new TarantoolCacheManager with the specified name.
   *
   * @param cachingProvider the CachingProvider that created the CacheManager
   * @param uri             the name of this cache manager
   * @param classLoader     the ClassLoader that should be used in converting values into Java Objects.
   * @param properties      the vendor specific Properties for the CacheManager
   * @throws NullPointerException if the URI and/or classLoader is null.
   */
  public TarantoolCacheManager(TarantoolCachingProvider cachingProvider, URI uri, ClassLoader classLoader, Properties properties) {
    this.cachingProvider = cachingProvider;

    if (uri == null) {
      throw new NullPointerException("No CacheManager URI specified");
    }
    this.uri = uri;

    if (classLoader == null) {
      throw new NullPointerException("No ClassLoader specified");
    }
    this.classLoaderReference = new WeakReference<ClassLoader>(classLoader);
    this.properties = new Properties();

    //this.properties = properties == null ? new Properties() : new Properties(properties);
    if (properties != null) {
      this.properties.putAll(properties);
    }

    session = new TarantoolSession(uri, classLoader);
    isClosed = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CachingProvider getCachingProvider() {
    return cachingProvider;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void close() {
    if (!isClosed()) {
      //first releaseCacheManager the CacheManager from the CacheProvider so that
      //future requests for this CacheManager won't return this one
      cachingProvider.releaseCacheManager(getURI(), getClassLoader());
      ArrayList<Cache<?, ?>> cacheList;
      synchronized (caches) {
        cacheList = new ArrayList<Cache<?, ?>>(caches.values());
        caches.clear();
      }
      for (Cache<?, ?> cache : cacheList) {
        try {
          cache.close();
        } catch (Exception e) {
          getLogger().log(Level.WARNING, "Error stopping cache: " + cache, e);
        }
      }
      session.close();
      isClosed = true;
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
  public URI getURI() {
    return uri;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Properties getProperties() {
    return properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ClassLoader getClassLoader() {
    return classLoaderReference.get();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) {
    ensureOpen();

    if (cacheName == null) {
      throw new NullPointerException("cacheName must not be null");
    }

    if (configuration == null) {
      throw new NullPointerException("configuration must not be null");
    }

    synchronized (caches) {
      if (caches.get(cacheName) == null) {
        TarantoolCache<K, V> cache = new TarantoolCache<K,V>(this, cacheName, getClassLoader(), session, configuration);
        caches.put(cache.getName(), cache);
        return cache;
      } else {
        throw new CacheException("A cache named " + cacheName + " already exists.");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
    ensureOpen();

    if (cacheName == null) {
        throw new NullPointerException("cacheName can not be null");
    }

    if (keyType == null) {
      throw new NullPointerException("keyType can not be null");
    }

    if (valueType == null) {
      throw new NullPointerException("valueType can not be null");
    }

    TarantoolCache<?, ?> cache = caches.get(cacheName);
    if (cache == null) {
      return null;
    } else {
      Configuration<?, ?> configuration = cache.getConfiguration(CompleteConfiguration.class);

      if (configuration.getKeyType() != null &&
          configuration.getKeyType().equals(keyType)) {

        if (configuration.getValueType() != null &&
            configuration.getValueType().equals(valueType)) {

          return (Cache<K, V>) cache;
        } else {
          throw new ClassCastException("Incompatible cache value types specified, expected " +
              configuration.getValueType() + " but " + valueType + " was specified");
        }
      } else {
        throw new ClassCastException("Incompatible cache key types specified, expected " +
            configuration.getKeyType() + " but " + keyType + " was specified");
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public <K, V> Cache<K, V> getCache(String cacheName) {
    ensureOpen();
    if (cacheName == null) {
        throw new NullPointerException();
    }
    return (Cache<K, V>) caches.get(cacheName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<String> getCacheNames() {
    ensureOpen();
    synchronized (caches) {
      HashSet<String> set = new HashSet<String>(caches.keySet());
      return Collections.unmodifiableSet(set);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroyCache(String cacheName) {
    ensureOpen();
    if (cacheName == null) {
      throw new NullPointerException();
    }

    Cache<?, ?> cache = caches.get(cacheName);

    if (cache != null) {
      cache.clear();
      cache.close();
    }
  }

  /**
   * Releases the Cache with the specified name from being managed by
   * this CacheManager.
   *
   * @param cacheName the name of the Cache to releaseCacheManager
   */
  void releaseCache(String cacheName) {
    if (cacheName == null) {
      throw new NullPointerException();
    }
    synchronized (caches) {
      caches.remove(cacheName);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableStatistics(String cacheName, boolean enabled) {
    ensureOpen();
    if (cacheName == null) {
      throw new NullPointerException();
    }
    TarantoolCache<?, ?> cache = caches.get(cacheName);
    if (cache == null) {
      throw new IllegalArgumentException("No such Cache named " + cacheName);
    }
    cache.setStatisticsEnabled(enabled);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enableManagement(String cacheName, boolean enabled) {
    ensureOpen();
    if (cacheName == null) {
      throw new NullPointerException();
    }
    TarantoolCache<?, ?> cache = caches.get(cacheName);
    if (cache == null) {
      throw new IllegalArgumentException("No such Cache named " + cacheName);
    }
    cache.setManagementEnabled(enabled);
  }

  @Override
  public <T> T unwrap(java.lang.Class<T> cls) {
    if (cls.isAssignableFrom(getClass())) {
      return cls.cast(this);
    }

    throw new IllegalArgumentException("Unwapping to " + cls + " is not a supported by this implementation");
  }

  /**
   * Obtain the logger.
   *
   * @return the logger.
   */
  Logger getLogger() {
    return LOGGER;
  }

}
