/**
 * Copyright 2018 Evgeniy Zaikin
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
package org.tarantool.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Evgeniy Zaikin
 */
public class NativeCache<K, V> implements Iterable<TarantoolTuple<K, V>> {

    /**
     * The {@link TarantoolSpace} is Space representation of this cache.
     */
    private final TarantoolSpace<K, V> space;

    /**
     * The {@link ExpiryTimeConverter} defines expiration policy.
     */
    private final ExpiryTimeConverter expiryPolicy;

    /**
     * The {@link TarantoolEventHandler} for create, update, remove, expire events.
     */
    private final TarantoolEventHandler<K, V> eventHandler;

    /**
     * The {@link CacheStore} for the Cache performs write-through/read-through operations.
     */
    private final CacheStore<K, V> cacheStore;

    /**
     * Constructs an {@link NativeCache} and bind it with an existing {@link TarantoolSpace}
     *
     * @param space        {@link TarantoolSpace} to be used with {@link NativeCache}
     * @param expiryPolicy used for obtaining Expiration Policy for Create, Update, Access
     * @param eventHandler {@link TarantoolEventHandler} for create, update, remove, expire events.
     * @param cacheStore   the {@link CacheStore} for the Cache performs write-through operations
     * @throws NullPointerException if a given space is null eventHandler is null
     */
    public NativeCache(TarantoolSpace<K, V> space,
                       ExpiryTimeConverter expiryPolicy,
                       TarantoolEventHandler<K, V> eventHandler,
                       CacheStore<K, V> cacheStore) {
        if (space == null || eventHandler == null) {
            throw new NullPointerException();
        }
        this.space = space;
        this.expiryPolicy = expiryPolicy;
        this.eventHandler = eventHandler;
        this.cacheStore = cacheStore;
    }

    /**
     * Select first (top) Tarantool's tuple from Space, build iterator,
     * which can be used to iterate next Tuple.
     *
     * @return an Iterator.
     */
    public Iterator<TarantoolTuple<K, V>> iterator() {
        return new TarantoolCursor<>(space, expiryPolicy, eventHandler, cacheStore);
    }

    /**
     * Clears the contents of the cache, without notifying listeners or
     * {@link CacheStore}s.
     */
    public void clear() {
        space.truncate();
    }

    /**
     * Perform Insert operation.
     *
     * @param key             the key
     * @param value           the value
     * @param creationTime    the time when the cache entry was created
     * @param useWriteThrough should write-through be used if it is configured
     * @return true if newly created value hasn't already expired, false otherwise
     * @throws NullPointerException if a given key is null
     */
    private boolean insert(K key, V value, long creationTime, boolean useWriteThrough) {
        if (key == null) {
            throw new NullPointerException();
        }
        long expiryTime = expiryPolicy.getExpiryForCreation(creationTime);
        // check that new entry is not already expired, in which case it should
        // not be added to the cache or listeners called or writers called.
        if (expiryTime > -1 && expiryTime <= creationTime) {
            return false;
        }

        if (useWriteThrough && cacheStore != null) {
            cacheStore.write(key, value);
        }

        space.insert(new TarantoolTuple<>(space, key, value, expiryTime));
        eventHandler.onCreated(key, value, value);
        return true;
    }

    /**
     * Replaces the entry for a key only if currently mapped to some
     * value.
     * <p>
     * If the cache is configured write-through, and this method returns true,
     * the associated {@link CacheStore#write(K, V)} method will be called.
     * </p>
     *
     * @param key   the key with which the specified value is associated
     * @param value the value to be associated with the specified key
     * @return <tt>true</tt> if the value was replaced
     * @throws NullPointerException if key is null or if value is null
     * @see CacheStore#write
     */
    public boolean replace(K key, V value) {
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }
        long now = System.currentTimeMillis();
        boolean result = false;
        TarantoolEntry<K, V> entry = new TarantoolEntry<>(space, expiryPolicy, eventHandler, cacheStore);
        try {
            if (entry.tryLock(key) && !entry.isExpiredAt(now)) {
                entry.update(value, now);
                result = true;
            }
        } finally {
            entry.unlock();
        }
        return result;
    }

    /**
     * Replaces the mapping for a key only if currently mapped to a
     * given value.
     * <p>
     *
     * @param oldValue value expected to be associated with the specified key
     * @param newValue value to be associated with the specified key
     * @return 1 if the value was replaced, -1 if not replaced, 0 if entry not found
     * @throws NullPointerException if key is null or if the values are null
     */
    public int replace(K key, V oldValue, V newValue) {
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
        int result = 0;
        TarantoolEntry<K, V> entry = new TarantoolEntry<>(space, expiryPolicy, eventHandler, cacheStore);
        try {
            if (entry.tryLock(key) && !entry.isExpiredAt(now)) {
                if (oldValue.equals(entry.getValue())) {
                    entry.update(newValue, now);
                    result = 1;

                } else {

                    entry.access(now);
                    result = -1;
                }
            }
        } finally {
            entry.unlock();
        }
        return result;
    }

    /**
     * Replaces the value for a given key if and only if there is a
     * value currently mapped by the key.
     * <p>
     * If the cache is configured write-through, and this method returns true,
     * the associated {@link CacheStore#write(K, V)} method will be called.
     * </p>
     *
     * @param key   key with which the specified value is associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with the specified key, or
     * <tt>null</tt> if there was no mapping for the key.
     * @throws NullPointerException if key is null or if value is null
     * @see CacheStore#write
     */
    public V getAndReplace(K key, V value) {
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        long now = System.currentTimeMillis();

        V result = null;
        TarantoolEntry<K, V> entry = new TarantoolEntry<>(space, expiryPolicy, eventHandler, cacheStore);
        try {
            if (entry.tryLock(key) && !entry.isExpiredAt(now)) {
                V oldValue = entry.getValue();
                entry.update(value, now);
                result = oldValue;
            }
        } finally {
            entry.unlock();
        }
        return result;
    }

    /**
     * Removes the mapping for a key from this cache if it is present.
     * <p>
     * More formally, if this cache contains a mapping from key <tt>k</tt> to
     * value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping is removed.
     * (The cache can contain at most one such mapping.)
     *
     * <p>Returns <tt>true</tt> if this cache previously associated the key,
     * or <tt>false</tt> if the cache contained no mapping for the key.
     * <p>
     * The cache will not contain a mapping for the specified key once the
     * call returns.
     * <p>
     * If the cache is configured write-through the associated
     * {@link CacheStore#delete(Object)} method will be called.
     * </p>
     *
     * @param key key whose mapping is to be removed from the cache
     * @return returns false if there was no matching key
     * @throws NullPointerException if key is null
     * @see CacheStore#delete
     */
    public boolean remove(K key) {
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        long now = System.currentTimeMillis();
        if (cacheStore != null) {
            cacheStore.delete(key);
        }
        TarantoolTuple<K, V> deletedTuple = space.delete(key);
        if (deletedTuple != null) {
            V oldValue = deletedTuple.getValue();
            eventHandler.onRemoved(key, oldValue, oldValue);
            return !deletedTuple.isExpiredAt(now);
        }
        return false;
    }

    /**
     * Removes the mapping for a key only if currently mapped to a
     * given value.
     * <p>
     *
     * @param oldValue value expected to be associated with the key
     * @return returns false if there was no matching key
     * @throws NullPointerException if the value is null
     */
    public boolean remove(K key, V oldValue) {
        if (key == null) {
            throw new NullPointerException();
        }
        if (oldValue == null) {
            throw new NullPointerException("null oldValue specified for key " + key);
        }
        boolean result = false;
        long now = System.currentTimeMillis();
        TarantoolEntry<K, V> entry = new TarantoolEntry<>(space, expiryPolicy, eventHandler, cacheStore);
        try {
            if (entry.tryLock(key) && !entry.isExpiredAt(now)) {
                if (oldValue.equals(entry.getValue())) {
                    entry.delete();
                    result = true;
                } else {
                    entry.access(now);
                }
            }
        } finally {
            entry.unlock();
        }
        return result;
    }

    /**
     * Determines if the cache contains an entry for the specified key.
     *
     * @param key key whose presence in this cache is to be tested.
     * @return <tt>true</tt> if this map contains a mapping for the specified key
     * @throws NullPointerException if key is null
     */
    public boolean containsKey(K key) {
        if (key == null) {
            throw new NullPointerException();
        }
        long now = System.currentTimeMillis();
        TarantoolTuple<K, V> tuple = space.select(key);
        return tuple != null && !tuple.isExpiredAt(now);
    }

    /**
     * Removes the entry for a key only if currently mapped to some
     * value.
     * <p>
     *
     * @param key key with which the specified value is associated
     * @return the value if one existed or null if no mapping existed for this key
     * @throws NullPointerException if the specified key or value is null.
     */
    public V getAndRemove(K key) {
        if (key == null) {
            throw new NullPointerException("null key specified");
        }

        long now = System.currentTimeMillis();
        if (cacheStore != null) {
            cacheStore.delete(key);
        }
        TarantoolTuple<K, V> tuple = space.delete(key);
        if (tuple != null && !tuple.isExpiredAt(now)) {
            V oldValue = tuple.getValue();
            eventHandler.onRemoved(key, oldValue, oldValue);
            return oldValue;
        }
        return null;
    }

    /**
     * A implementation of PutAll that allows optional replacement of existing
     * values and optionally writing values when Write Through is configured.
     *
     * @param map                   the Map of entries to put
     * @param replaceExistingValues should existing values be replaced by those in
     *                              the map?
     * @param useWriteThrough       should write-through be used if it is configured
     * @return performed put count
     */
    public int putAll(Map<? extends K, ? extends V> map,
                      final boolean replaceExistingValues,
                      final boolean useWriteThrough) {
        long now = System.currentTimeMillis();
        int putCount = 0;

        HashMap<? extends K, ? extends V> entriesToPut = new HashMap<>(map);
        for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
            if (entry.getKey() == null) {
                throw new NullPointerException("key");
            }
            if (entry.getValue() == null) {
                throw new NullPointerException("key " + entry.getKey() + " has a null value");
            }
        }

        if (cacheStore != null && useWriteThrough) {
            cacheStore.writeAll(entriesToPut);
        }

        //perform the put
        for (Entry<? extends K, ? extends V> entry : entriesToPut.entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();

            if (replaceExistingValues) {
                TarantoolEntry<K, V> entryToLoad = new TarantoolEntry<>(space, expiryPolicy, eventHandler, null);
                if (entryToLoad.tryLock(key)) {
                    entryToLoad.update(value, now);
                    putCount++;
                } else if (insert(key, value, now, false)) {
                    putCount++;
                }
            } else {
                if (insert(key, value, now, false)) {
                    putCount++;
                }
            }
        }
        return putCount;
    }

    /**
     * Associates the specified value with the specified key in the cache.
     * <p>
     * If the cache previously contained a mapping for the key, the old
     * value is replaced by the specified value.
     * <p>
     * If the cache is configured write-through the
     * {@link CacheStore#write(K, V)} method will be called.
     * </p>
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return <tt>true</tt> if the value was set
     * @throws NullPointerException if key is null or if value is null
     * @see CacheStore#write
     */
    public boolean put(K key, V value) {
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        long now = System.currentTimeMillis();
        TarantoolEntry<K, V> entry = new TarantoolEntry<>(space, expiryPolicy, eventHandler, cacheStore);
        if (entry.tryLock(key)) {
            entry.update(value, now);
            return true;
        }
        return insert(key, value, now, true);
    }

    /**
     * Associates the specified key with the given value if it is
     * not already associated with a value.
     * <p>
     * If the cache is configured write-through, and this method returns true,
     * the associated {@link CacheStore#write(K, V)} method will be called.
     * </p>
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return true if a value was set.
     * @throws NullPointerException if key is null or value is null
     * @see CacheStore#write
     */
    public boolean putIfAbsent(K key, V value) {
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        /*
         * 2-Phase insert protocol.
         * Temporary set expire time that equals to creationTime,
         * inserting tuple is considered expired before second phase performed.
         */
        long creationTime = System.currentTimeMillis();
        TarantoolTuple<K, V> tuple = new TarantoolTuple<>(space, key, value, creationTime);
        // TODO: use another field to store Lock state
        try {
            space.insert(tuple);
        } catch (TarantoolCacheException e) {
            return false;
        }

        long expiryTime = expiryPolicy.getExpiryForCreation(creationTime);
        // check that new entry is not already expired, in which case it should
        // not be added to the cache or listeners called or writers called.
        if (expiryTime > -1 && expiryTime <= creationTime) {
            space.delete(key);
            return false;
        }

        if (cacheStore != null) {
            try {
                cacheStore.write(key, value);
            } catch (Exception e) {
                space.delete(key);
                throw e;
            }
        }

        tuple.updateExpiry(expiryTime);
        eventHandler.onCreated(key, value, value);
        return true;
    }

    /**
     * Gets the value for the specified key from the underlying cache, including
     * attempting to load it if a {@link CacheStore} is configured (with read-through).
     * <p>
     * Any events that need to be raised are added to the specified dispatcher.
     * </p>
     *
     * @param key the key of the entry to get from the cache
     * @return the value loaded
     * @throws NullPointerException if key is null
     */
    public V get(K key) {
        if (key == null) {
            throw new NullPointerException();
        }
        long now = System.currentTimeMillis();

        V value = null;
        TarantoolEntry<K, V> entry = new TarantoolEntry<>(space, expiryPolicy, eventHandler, null);
        try {
            if (!entry.tryLock(key)) {

                if (cacheStore != null) {
                    value = cacheStore.load(key);
                }

                if (value != null) {
                    insert(key, value, now, false);
                }

            } else if (entry.isExpiredAt(now)) {

                if (cacheStore != null) {
                    value = cacheStore.load(key);
                }

                if (value != null) {
                    entry.update(value, now);
                } else {
                    entry.expire();
                }

            } else {
                entry.access(now);
                return entry.getValue();
            }

        } finally {
            entry.unlock();
        }
        return value;
    }

    /**
     * Associates the specified value with the specified key in this cache,
     * returning an existing value if one existed.
     * <p>
     * If the cache previously contained a mapping for
     * the key, the old value is replaced by the specified value.
     * <p>
     * The previous value is returned, or null if there was no value associated
     * with the key previously.</p>
     * <p>
     * If the cache is configured write-through the associated
     * {@link CacheStore#write(K, V)} method will be called.
     * </p>
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the value associated with the key at the start of the operation or
     * null if none was associated
     * @throws NullPointerException if key is null or if value is null
     * @see CacheStore#write
     */
    public V getAndPut(K key, V value) {
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        if (value == null) {
            throw new NullPointerException("null value specified for key " + key);
        }

        long now = System.currentTimeMillis();

        V result = null;
        TarantoolEntry<K, V> entry = new TarantoolEntry<>(space, expiryPolicy, eventHandler, cacheStore);
        if (entry.tryLock(key)) {
            try {
                if (!entry.isExpiredAt(now)) {
                    result = entry.getValue();
                }
                entry.update(value, now);
            } finally {
                entry.unlock();
            }
        } else {
            insert(key, value, now, true);
        }
        return result;
    }

    /**
     * Removes all of the mappings from this cache.
     * <p>
     * The order that the individual entries are removed is undefined.
     * <p>
     * For every mapping that exists the following are called:
     * <ul>
     * <li>any registered {@link TarantoolEventHandler}s</li>
     * <li>if the cache is a write-through cache, the {@link CacheStore}</li>
     * </ul>
     * If the cache is empty, the {@link CacheStore} is not called.
     * <p>
     * This is potentially an expensive operation as listeners are invoked.
     * Use {@link #clear()} to avoid this.
     *
     * @param useWriteThrough should write-through be used if it is configured
     * @return removals count
     * @see CacheStore#deleteAll
     */
    public int removeAll(final boolean useWriteThrough) {
        int cacheRemovals = 0;
        long now = System.currentTimeMillis();
        HashSet<K> allExpiredKeys = new HashSet<>();
        HashSet<K> allNonExpiredKeys = new HashSet<>();
        HashSet<K> keysToDelete = new HashSet<>();

        boolean isWriteThrough = useWriteThrough && cacheStore != null;
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
            cacheStore.deleteAll(keysToDelete);
        }

        //remove the deleted keys that were successfully deleted from the set (only non-expired)
        for (K key : allNonExpiredKeys) {
            if (!keysToDelete.contains(key)) {
                TarantoolTuple<K, V> deletedTuple = space.delete(key);
                if (deletedTuple != null) {
                    V oldValue = deletedTuple.getValue();
                    eventHandler.onRemoved(key, oldValue, oldValue);
                    cacheRemovals++;
                }
            }
        }

        //remove the deleted keys that were successfully deleted from the set (only expired)
        for (K key : allExpiredKeys) {
            if (!keysToDelete.contains(key)) {
                TarantoolTuple<K, V> deletedTuple = space.delete(key);
                if (deletedTuple != null) {
                    V oldValue = deletedTuple.getValue();
                    eventHandler.onRemoved(key, oldValue, oldValue);
                }
            }
        }

        return cacheRemovals;
    }

    /**
     * Removes entries for the specified keys.
     * <p>
     * The order in which the individual entries are removed is undefined.
     * <p>
     * For every entry in the key set, the following are called:
     * <ul>
     * <li>any registered {@link TarantoolEventHandler}s</li>
     * <li>if the cache is a write-through cache, the {@link CacheStore}</li>
     * </ul>
     *
     * @param keys            the keys to remove
     * @param useWriteThrough should write-through be used if it is configured
     * @return removals count
     * @throws NullPointerException if keys is null or if it contains a null key
     * @see CacheStore#deleteAll
     */
    public int removeAll(Set<? extends K> keys, final boolean useWriteThrough) {
        int cacheRemovals = 0;
        if (keys.isEmpty()) {
            return cacheRemovals;
        }
        for (K key : keys) {
            if (key == null) {
                throw new NullPointerException("keys contains a null");
            }
        }

        //call write-through on deleted entries
        if (useWriteThrough && cacheStore != null) {
            HashSet<K> deletedKeys = new HashSet<>(keys);
            cacheStore.deleteAll(deletedKeys);

            //At this point, deletedKeys will contain only those that were _not_ deleted
            //Now delete only those that the writer deleted
            for (K key : keys) {
                //only delete those keys that the writer deleted
                if (!deletedKeys.contains(key)) {
                    TarantoolTuple<K, V> deletedTuple = space.delete(key);
                    if (deletedTuple != null) {
                        V oldValue = deletedTuple.getValue();
                        eventHandler.onRemoved(key, oldValue, oldValue);
                        cacheRemovals++;
                    }
                }
            }
        } else {

            for (K key : keys) {
                TarantoolTuple<K, V> deletedTuple = space.delete(key);
                if (deletedTuple != null) {
                    V oldValue = deletedTuple.getValue();
                    eventHandler.onRemoved(key, oldValue, oldValue);
                    cacheRemovals++;
                }
            }
        }

        return cacheRemovals;
    }

    /**
     * Invokes an {@link MutableEntryOperation} against the entry specified by
     * the provided key.
     *
     * @param operation {@link MutableEntryOperation} that defines operation
     * @param key       the key to the entry
     * @param newValue  the newValue to be set
     * @throws NullPointerException if key is null
     */
    public void invoke(MutableEntryOperation operation, K key, V newValue) {
        if (key == null) {
            throw new NullPointerException("null key specified");
        }
        long now = System.currentTimeMillis();
        switch (operation) {
            case NONE:
                break;

            case CREATE:
                insert(key, newValue, now, true);
                break;

            case LOAD:
                TarantoolEntry<K, V> entryToLoad = new TarantoolEntry<>(space, expiryPolicy, eventHandler, null);
                if (entryToLoad.tryLock(key)) {
                    entryToLoad.update(newValue, now);
                    entryToLoad.unlock();
                } else {
                    insert(key, newValue, now, false);
                }
                break;

            case UPDATE:
                TarantoolEntry<K, V> entry = new TarantoolEntry<>(space, expiryPolicy, eventHandler, cacheStore);
                if (entry.tryLock(key)) {
                    entry.update(newValue, now);
                    entry.unlock();
                }
                break;

            case REMOVE:
                remove(key);
                break;

            default:
                break;
        }
    }
}
