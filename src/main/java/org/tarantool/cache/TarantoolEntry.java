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

import java.util.Arrays;
import java.util.List;

public class TarantoolEntry<K, V> {

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
     * The {@link TarantoolSpace} is space where {@link TarantoolTuple} is stored.
     */
    private final TarantoolSpace<K, V> space;

    /**
     * The {@link TarantoolTuple} holds current tuple.
     */
    private TarantoolTuple<K, V> tuple;

    /**
     * Constructs an {@link TarantoolEntry}
     *
     * @param space        {@link TarantoolSpace} where {@link TarantoolEntry} is stored.
     * @param expiryPolicy used for obtaining Expiration Policy for Create, Update, Access
     * @param eventHandler {@link TarantoolEventHandler} for create, update, remove, expire events.
     * @param cacheStore   the {@link CacheStore} for the Cache performs write-through operations
     * @throws NullPointerException if a given space is null
     */
    TarantoolEntry(TarantoolSpace<K, V> space,
                   ExpiryTimeConverter expiryPolicy,
                   TarantoolEventHandler<K, V> eventHandler,
                   CacheStore<K, V> cacheStore) {
        if (space == null) {
            throw new NullPointerException();
        }
        this.space = space;
        this.expiryPolicy = expiryPolicy;
        this.eventHandler = eventHandler;
        this.cacheStore = cacheStore;
    }

    /**
     * Try lock tuple.
     *
     * @return true if tuple with given key exists.
     * @throws NullPointerException if a given key is null
     */
    boolean tryLock(K key) {
        if (key == null) {
            throw new NullPointerException();
        }
        // TODO: move update operation array to TarantoolTuple implementation
        List<?> response = space.update(key, (Object) Arrays.asList("=", 3, true));
        if (response.isEmpty()) {
            return false;
        }
        tuple = new TarantoolTuple<>(space, (List<?>) response.iterator().next());
        return true;
    }

    /**
     * Unlock given tuple.
     */
    void unlock() {
        tuple = null;
    }

    /**
     * Determines if the Cache Entry associated with this value would be expired
     * at the specified time
     *
     * @param now time in milliseconds (since the Epoch)
     * @return true if the value would be expired at the specified time
     * @throws IllegalStateException if tuple is not locked
     */
    boolean isExpiredAt(long now) {
        if (tuple == null) {
            throw new IllegalStateException("Cannot update the tuple, tuple is not locked");
        }
        return tuple.isExpiredAt(now);
    }

    /**
     * Performs the full update operation (value and expiryTime).
     *
     * @param value            the Value
     * @param modificationTime time in milliseconds (since the Epoch)
     * @throws IllegalStateException if tuple is not locked
     */
    void update(V value, long modificationTime) {
        if (tuple == null) {
            throw new IllegalStateException("Cannot update the tuple, tuple is not locked");
        }
        K key = tuple.getKey();
        V oldValue = tuple.getValue();
        long expiryTime;
        // even if the tuple exists we should check whether it is not expired,
        // and if it is, we don't delete expired tuple here,
        // performing forced update with creation time instead
        if (tuple.isExpiredAt(modificationTime)) {
            expiryTime = expiryPolicy.getExpiryForCreation(modificationTime);
        } else {
            expiryTime = expiryPolicy.getExpiryForUpdate(modificationTime);
        }

        if (expiryTime != -1) {
            // Check whether Tuple with new expiryTime becomes expired
            if (expiryTime <= modificationTime) {
                // delete expired tuple right here
                expire();
            } else {
                // set new value, set new calculated expiryTime for update
                tuple.setValue(value);
                tuple.setExpiryTime(expiryTime);
                space.replace(tuple);
                if (cacheStore != null) {
                    cacheStore.write(key, value);
                }
                if (eventHandler != null) {
                    eventHandler.onUpdated(key, value, oldValue);
                }
            }
        } else {
            //leave the expiry time untouched when expiryTime is undefined
            tuple.updateValue(value);
            if (cacheStore != null) {
                cacheStore.write(key, value);
            }
            if (eventHandler != null) {
                eventHandler.onUpdated(key, value, oldValue);
            }
        }
    }

    /**
     * Deletes tuple from the Space.
     * Stores fields of the deleted tuple to internal structure.
     * Calls appropriate event.
     *
     * @throws IllegalStateException if tuple is not locked
     */
    void delete() {
        if (tuple == null) {
            throw new IllegalStateException("Cannot delete the tuple, tuple is not locked");
        }
        if (cacheStore != null) {
            cacheStore.delete(tuple.getKey());
        }
        K oldKey = tuple.getKey();
        V oldValue = tuple.getValue();
        space.delete(oldKey);
        if (eventHandler != null) {
            eventHandler.onRemoved(oldKey, oldValue, oldValue);
        }
    }

    /**
     * Delete expired Tarantool's tuple.
     * Stores fields of the deleted tuple to internal structure.
     * Calls appropriate event.
     *
     * @throws IllegalStateException if tuple is not locked
     */
    void expire() {
        if (tuple == null) {
            throw new IllegalStateException("Tuple is not locked");
        }
        K expiredKey = tuple.getKey();
        V expiredValue = tuple.getValue();
        space.delete(expiredKey);
        if (eventHandler != null) {
            eventHandler.onExpired(expiredKey, expiredValue, expiredValue);
        }
    }

    /**
     * Updates the access time to that which is specified.
     * If expire for access is undefined - leave untouched.
     *
     * @param accessTime the time when the value was accessed
     * @throws IllegalStateException if tuple is not locked
     */
    void access(long accessTime) {
        if (tuple == null) {
            throw new IllegalStateException("Cannot access the tuple, tuple is not locked");
        }
        long expiryTime = expiryPolicy.getExpiryForAccess(accessTime);
        if (expiryTime != -1) {
            // Check whether Tuple with new expiryTime becomes expired
            if (expiryTime <= accessTime) {
                // delete expired tuple right here
                expire();
            } else {
                // set new calculated expiryTime for access
                tuple.updateExpiry(expiryTime);
            }
        }
    }

    /**
     * Returns the key corresponding to the locked tuple.
     *
     * @return the key corresponding to the locked tuple
     * @throws IllegalStateException if tuple is not locked
     */
    public K getKey() {
        if (tuple == null) {
            throw new IllegalStateException("Cannot access the tuple, tuple is not locked");
        }
        return tuple.getKey();
    }

    /**
     * Returns the value corresponding to the locked tuple.
     *
     * @return the value corresponding to the locked tuple
     * @throws IllegalStateException if tuple is not locked
     */
    public V getValue() {
        if (tuple == null) {
            throw new IllegalStateException("Cannot access the tuple, tuple is not locked");
        }
        return tuple.getValue();
    }
}
