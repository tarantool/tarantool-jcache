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

package org.tarantool.jsr107.event;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;

/**
 * The implementation of the {@link CacheEntryEvent}.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of cached values
 * @author Greg Luck
 * @author Evgeniy Zaikin
 * @since 1.0
 */
public class TarantoolCacheEntryEvent<K, V> extends CacheEntryEvent<K, V> {
  private static final long serialVersionUID = 7240204723819450535L;

  private final K key;
  private final V value;
  private final V oldValue;
  private final boolean oldValueAvailable;

  /**
   * Constructs a cache entry event from a given cache as source
   * (without an old value)
   *
   * @param source the cache that originated the event
   * @param key    the key
   * @param value  the value
   */
  public TarantoolCacheEntryEvent(Cache<K, V> source, K key, V value, EventType eventType) {
    super(source, eventType);
    this.key = key;
    this.value = value;
    this.oldValue = null;
    this.oldValueAvailable = false;
  }

  /**
   * Constructs a cache entry event from a given cache as source
   * (with an old value)
   *
   * @param source   the cache that originated the event
   * @param key      the key
   * @param value    the value
   * @param oldValue the oldValue
   */
  public TarantoolCacheEntryEvent(Cache<?, ?> source, K key, V value, V oldValue, EventType eventType) {
    super(source, eventType);
    this.key = key;
    this.value = value;
    this.oldValue = oldValue;
    this.oldValueAvailable = true;
  }

  /**
   * Constructs a cache entry event from a given cache as source
   * with an old value, explicitly specifying whether old value is available
   *
   * @param source            the cache that originated the event
   * @param key               the key
   * @param value             the value
   * @param oldValue          the oldValue
   * @param oldValueAvailable indicates whether old value is available
   */
  public TarantoolCacheEntryEvent(Cache<?, ?> source, K key, V value, V oldValue, EventType eventType, boolean oldValueAvailable) {
    super(source, eventType);
    this.key = key;
    this.value = value;
    this.oldValue = oldValue;
    this.oldValueAvailable = oldValueAvailable;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public K getKey() {
    return key;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V getValue() {
    return value;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V getOldValue() throws UnsupportedOperationException {
    if (isOldValueAvailable()) {
      return oldValue;
    } else {
      return null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz != null && clazz.isInstance(this)) {
      return (T) this;
    } else {
      throw new IllegalArgumentException("The class " + clazz + " is unknown to this implementation");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isOldValueAvailable() {
    return oldValueAvailable;
  }
}
