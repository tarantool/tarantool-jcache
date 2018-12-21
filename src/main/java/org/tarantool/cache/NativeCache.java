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

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Evgeniy Zaikin
 */
public class NativeCache<K, V> implements Iterable<TarantoolTuple<K, V>>, Closeable {

    private static enum CursorType {
        UNDEFINED,
        SERVER,
        CLIENT
    }

    /**
     * The {@link CursorType} represents current state of {@link NativeCache}
     */
    private CursorType cursorType = CursorType.UNDEFINED;

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
     * Current tuple container, consist of key, value, expiryTime
     */
    private final TarantoolTuple<K, V> tuple;

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
        this.tuple = new TarantoolTuple<K, V>(space);
    }

    /**
     * Select Tarantool's tuple and set {@link NativeCache} to it
     *
     * @param key which is used for select tuple
     * @return true if a tuple with a given key exists
     * @throws NullPointerException if a given key is null
     */
    private boolean locate(K key) {
        if (key == null) {
            throw new NullPointerException();
        }
        final Iterator<?> iterator = space.select(key).iterator();
        if (iterator.hasNext()) {
            cursorType = CursorType.SERVER;
            tuple.assign((List<?>) iterator.next());
            return true;
        }
        cursorType = CursorType.UNDEFINED;
        return false;
    }

    /**
     * Select first (top) Tarantool's tuple from Space, build iterator,
     * which can be used to iterate next Tuple (server-side cursor).
     *
     * @return an Iterator.
     */
    public Iterator<TarantoolTuple<K, V>> iterator() {
        Iterator<TarantoolTuple<K, V>> iterator = new Iterator<TarantoolTuple<K, V>>() {
            private Iterator<?> iterator = space.first().iterator();
            private final long now = System.currentTimeMillis();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public TarantoolTuple<K, V> next() {
                if (iterator.hasNext()) {
                    // It is still current tuple, parse it
                    tuple.assign((List<?>) iterator.next());
                    // Set cursor type to CursorType.SERVER
                    cursorType = CursorType.SERVER;
                    // Update tuple access time
                    access(now);
                    // Fetch next tuple for the future, do not parse it now
                    iterator = space.next(tuple.getKey()).iterator();
                    // Return current parsed tuple
                    return tuple;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                if (cursorType != CursorType.SERVER) {
                    throw new IllegalStateException("Must progress to the next entry to remove");
                } else {
                    if (cacheStore != null) {
                        cacheStore.delete(tuple.getKey());
                    }
                    delete(tuple.getKey());
                }
            }
        };

        // Set cursor type to CursorType.UNDEFINED, until progress to the first entry
        cursorType = CursorType.UNDEFINED;
        tuple.invalidate();
        return iterator;
    }

    /**
     * Clears the contents of the cache, without notifying listeners or
     * {@link CacheStore}s.
     */
    public void clear() {
        space.truncate();
    }

    /**
     * Delete Tarantool's tuple by given key, and if succeeded set {@link NativeCache} to
     * deleted tuple. Cursor's state is changed to CursorType.CLIENT,
     * it means that cursor is detached from the Space.
     *
     * @param key which is used for select tuple
     * @return true if a tuple with a given key exists in space and delete operation succeed
     * @throws NullPointerException if a given key is null
     */
    private boolean delete(K key) {
        if (key == null) {
            throw new NullPointerException();
        }

        final Iterator<?> iterator = space.delete(key).iterator();
        if (iterator.hasNext()) {
            // Change cursor type to forbid attempt to remove tuple again
            cursorType = CursorType.CLIENT;
            tuple.assign((List<?>) iterator.next());
            V oldValue = tuple.getValue();
            //TODO: check if entry was expired, and if it was - call onExpired(...) instead
            eventHandler.onRemoved(key, oldValue, oldValue);
            return true;
        }
        cursorType = CursorType.UNDEFINED;
        return false;
    }

    /**
     * Delete expired Tarantool's tuple, and if succeeded set {@link NativeCache} to
     * deleted tuple. Cursor's state is changed to CursorType.CLIENT,
     * it means that cursor is detached from the Space.
     *
     * @throws IllegalStateException if cursor is not opened in server mode
     */
    private void expire() {
        if (cursorType != CursorType.SERVER) {
            throw new IllegalStateException("Cursor is not opened in Server Mode");
        }
        delete(tuple.getKey());
    }

    /**
     * Perform Insert operation, and if succeeded set {@link NativeCache} to
     * inserted tuple. Cursor's state is changed to CursorType.SERVER,
     * it means that cursor is active and attached to the Space.
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

        tuple.setKey(key);
        tuple.setValue(value);
        tuple.setExpiryTime(expiryTime);

        space.insert(tuple);
        cursorType = CursorType.SERVER;
        eventHandler.onCreated(key, value, value);
        return true;
    }

    /**
     * Sets the value with the additional side-effect of updating the
     * modification time to that which is specified.
     *
     * @param newValue         the new value
     * @param modificationTime the time when the value was modified
     * @param useWriteThrough  should write-through be used if it is configured
     * @throws IllegalStateException if cursor is not opened in server mode
     */
    private void update(V newValue, long modificationTime, boolean useWriteThrough) {
        if (cursorType != CursorType.SERVER) {
            throw new IllegalStateException("Cursor is not opened in Server Mode");
        }

        long expiryTime = -1;
        // even if the tuple exists we should check whether it is not expired,
        // and if it is, we don't delete expired tuple here,
        // performing forced update with creation time instead
        if (tuple.isExpiredAt(modificationTime)) {
            expiryTime = expiryPolicy.getExpiryForCreation(modificationTime);
        } else {
            expiryTime = expiryPolicy.getExpiryForUpdate(modificationTime);
        }

        K key = tuple.getKey();
        V oldValue = tuple.getValue();

        tuple.setValue(newValue);

        if (expiryTime != -1) {
            // set new calculated expiryTime
            tuple.setExpiryTime(expiryTime);
            // And check whether Tuple with new expiryTime becomes expired
            // and if it is, we must delete expired tuple right here
            if (!tuple.isExpiredAt(modificationTime)) {
                tuple.update();
                eventHandler.onUpdated(key, newValue, oldValue);
            } else {
                expire();
            }
        } else {
            //leave the expiry time untouched when expiryTime is undefined
            tuple.updateValue();
            eventHandler.onUpdated(key, newValue, oldValue);
        }
        if (useWriteThrough && cacheStore != null) {
            cacheStore.write(key, newValue);
        }
    }

    /**
     * Updates the access time to that which is specified.
     *
     * @param accessTime the time when the value was accessed
     * @throws IllegalStateException if cursor is not opened in server mode
     */
    private void access(long accessTime) {
        if (cursorType != CursorType.SERVER) {
            throw new IllegalStateException("Cursor is not opened in Server Mode");
        }
        long expiryTime = expiryPolicy.getExpiryForAccess(accessTime);
        if (expiryTime != -1) {
            // set new calculated expiryTime for access
            tuple.setExpiryTime(expiryTime);
            // And check whether Tuple with new expiryTime becomes expired
            // and if it is, we must delete expired tuple right here
            if (!tuple.isExpiredAt(accessTime)) {
                tuple.updateExpiry();
            } else {
                expire();
            }
        }
    }

    /**
     * Closes cursor, detaching from Tarantool's space
     */
    public void close() {
        cursorType = CursorType.UNDEFINED;
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
     * @see #getAndReplace(Object, Object)
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
        try {
            if (locate(key) && !tuple.isExpiredAt(now)) {
                update(value, now, true);
                result = true;
            }
        } finally {
            close();
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
        if (newValue == null) {
            throw new NullPointerException("null newValue specified for key " + tuple.getKey());
        }
        if (oldValue == null) {
            throw new NullPointerException("null oldValue specified for key " + tuple.getKey());
        }
        long now = System.currentTimeMillis();
        int result = 0;
        try {
            if (locate(key) && !tuple.isExpiredAt(now)) {
                if (oldValue.equals(tuple.getValue())) {
                    update(newValue, now, true);
                    result = 1;

                } else {

                    access(now);
                    result = -1;
                }
            }
        } finally {
            close();
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
        try {
            if (locate(key) && !tuple.isExpiredAt(now)) {
                V oldValue = tuple.getValue();
                update(value, now, true);
                result = oldValue;
            }
        } finally {
            close();
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
        return delete(key) && !tuple.isExpiredAt(now);
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
        if (oldValue == null) {
            throw new NullPointerException("null oldValue specified for key " + tuple.getKey());
        }
        long now = System.currentTimeMillis();

        if (locate(key) && !tuple.isExpiredAt(now)) {
            if (oldValue.equals(tuple.getValue())) {
                if (cacheStore != null) {
                    cacheStore.delete(tuple.getKey());
                }
                delete(tuple.getKey());
                return true;

            } else {

                access(now);
                close();
                return false;
            }
        }
        close();
        return false;
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
        boolean result;
        try {
            result = locate(key) && !tuple.isExpiredAt(now);
        } finally {
            close();
        }
        return result;
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
        if (delete(key) && !tuple.isExpiredAt(now)) {
            return tuple.getValue();
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
                if (locate(key)) {
                    update(value, now, false);
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
     * value is replaced by the specified value.  (A cache <tt>c</tt> is said to
     * contain a mapping for a key <tt>k</tt> if and only if {@link
     * #containsKey(Object) c.containsKey(k)} would return <tt>true</tt>.)
     * <p>
     * If the cache is configured write-through the
     * {@link CacheStore#write(K, V)} method will be called.
     * </p>
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return <tt>true</tt> if the value was set
     * @throws NullPointerException if key is null or if value is null
     * @see java.util.Map#put(Object, Object)
     * @see #getAndPut(Object, Object)
     * @see #getAndReplace(Object, Object)
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
        if (!locate(key)) {
            return insert(key, value, now, true);
        }
        update(value, now, true);
        return true;
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
        long now = System.currentTimeMillis();
        return !locate(key) && insert(key, value, now, true);
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
     */
    public V get(K key) {
        long now = System.currentTimeMillis();

        V value = null;
        try {
            if (!locate(key)) {

                if (cacheStore != null) {
                    value = cacheStore.load(key);
                }

                if (value != null) {
                    insert(key, value, now, false);
                }

            } else if (tuple.isExpiredAt(now)) {

                if (cacheStore != null) {
                    value = cacheStore.load(key);
                }

                if (value != null) {
                    update(value, now, false);
                } else {
                    expire();
                }

            } else {
                access(now);
                return tuple.getValue();
            }

        } finally {
            close();
        }
        return value;
    }

    /**
     * Associates the specified value with the specified key in this cache,
     * returning an existing value if one existed.
     * <p>
     * If the cache previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A cache
     * <tt>c</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) c.containsKey(k)} would return
     * <tt>true</tt>.)
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
     * @see #put(Object, Object)
     * @see #getAndReplace(Object, Object)
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
        try {

            if (locate(key)) {
                if (!tuple.isExpiredAt(now)) {
                    result = tuple.getValue();
                }
                update(value, now, true);
            } else {
                insert(key, value, now, true);
            }

        } finally {
            close();
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
     * @see #clear()
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
                if (delete(key)) {
                    cacheRemovals++;
                }
            }
        }

        //remove the deleted keys that were successfully deleted from the set (only expired)
        for (K key : allExpiredKeys) {
            if (!keysToDelete.contains(key)) {
                delete(key);
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
                    if (delete(key)) {
                        cacheRemovals++;
                    }
                }
            }
        } else {

            for (K key : keys) {
                if (delete(key)) {
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
                if (locate(key)) {
                    update(newValue, now, false);
                } else {
                    insert(key, newValue, now, false);
                }
                break;

            case UPDATE:
                if (locate(key)) {
                    update(newValue, now, true);
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
