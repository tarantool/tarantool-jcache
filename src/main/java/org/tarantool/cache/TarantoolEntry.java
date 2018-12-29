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

/**
 * TarantoolEntry provides functional over the {@link TarantoolTuple},
 * receives expiryTime from {@link ExpiryTimeConverter}.
 * Push, update methods can be used with or without write-through.
 * If write-through is used - method calls {@link CacheStore#write}.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Evgeniy Zaikin
 */
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
     * @throws NullPointerException if a given expiryPolicy is null
     * @throws NullPointerException if a given eventHandler is null
     */
    TarantoolEntry(TarantoolSpace<K, V> space,
                   ExpiryTimeConverter expiryPolicy,
                   TarantoolEventHandler<K, V> eventHandler,
                   CacheStore<K, V> cacheStore) {
        if (space == null || expiryPolicy == null || eventHandler == null) {
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
     * @param key the key to be locked
     * @return true if tuple with given key exists.
     * @throws NullPointerException if a given key is null
     */
    boolean tryLock(K key) {
        if (key == null) {
            throw new NullPointerException();
        }
        tuple = new TarantoolTuple<>(space, key).tryLock();
        return tuple != null;
    }

    /**
     * Performs two-phase insert (tryLock + commit).
     *
     * @param key          the key to be locked
     * @param value        the Value
     * @param creationTime time in milliseconds (since the Epoch)
     * @return true if two-phase insert succeeded
     * @throws NullPointerException if a given key is null
     */
    boolean tryPush(K key, V value, long creationTime) {
        if (key == null) {
            throw new NullPointerException();
        }
        TarantoolTuple<K, V> tuple = new TarantoolTuple<>(space, key);
        if (!tuple.tryPush()) {
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

        tuple.update(value, expiryTime);
        eventHandler.onCreated(key, value, value);
        return true;
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
            throw new IllegalStateException("Cannot check the tuple, tuple is not locked");
        }
        return tuple.isExpiredAt(now);
    }

    /**
     * Inserts the tuple in one step without any lock.
     *
     * @param key          the key
     * @param value        the value
     * @param creationTime the time when the cache entry was created
     * @return true if newly created value hasn't already expired, false otherwise
     * @throws NullPointerException if a given key is null
     */
    boolean create(K key, V value, long creationTime) {
        if (key == null) {
            throw new NullPointerException();
        }
        long expiryTime = expiryPolicy.getExpiryForCreation(creationTime);
        // check that new entry is not already expired, in which case it should
        // not be added to the cache or listeners called or writers called.
        if (expiryTime > -1 && expiryTime <= creationTime) {
            return false;
        }

        if (cacheStore != null) {
            cacheStore.write(key, value);
        }

        space.insert(new TarantoolTuple<>(space, key, value, expiryTime));
        eventHandler.onCreated(key, value, value);
        return true;
    }

    /**
     * Performs the full update operation (value and expiryTime).
     *
     * @param value            the Value
     * @param modificationTime time in milliseconds (since the Epoch)
     * @return the value before update
     * @throws IllegalStateException if tuple is not locked
     */
    V update(V value, long modificationTime) {
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
                eventHandler.onUpdated(key, value, oldValue);
            }
        } else {
            //leave the expiry time untouched when expiryTime is undefined
            tuple.updateValue(value);
            if (cacheStore != null) {
                cacheStore.write(key, value);
            }
            eventHandler.onUpdated(key, value, oldValue);
        }
        return oldValue;
    }

    /**
     * Delete expired Tarantool's tuple.
     * Stores fields of the deleted tuple to internal structure.
     * Calls appropriate event.
     *
     * @return the expired value
     * @throws IllegalStateException if tuple is not locked
     */
    V expire() {
        if (tuple == null) {
            throw new IllegalStateException("Cannot delete the tuple");
        }
        K expiredKey = tuple.getKey();
        V expiredValue = tuple.getValue();
        space.delete(expiredKey);
        eventHandler.onExpired(expiredKey, expiredValue, expiredValue);
        return expiredValue;
    }

    /**
     * Updates the access time to that which is specified.
     * If expire for access is undefined - leave untouched.
     *
     * @param accessTime the time when the value was accessed
     * @return the value corresponding to the accessed tuple
     * @throws IllegalStateException if tuple is not locked
     */
    V access(long accessTime) {
        if (tuple == null) {
            throw new IllegalStateException("Cannot access the tuple");
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
        return tuple.getValue();
    }

    /**
     * Indicates whether some value is equal to tuple's one.
     *
     * @param value the value to match
     * @return true if a given value is equal to tuple's value
     * @throws IllegalStateException if tuple is not locked
     * @throws NullPointerException  if a given value is null
     */
    public boolean isMatch(V value) {
        if (tuple == null) {
            throw new IllegalStateException("Cannot access the tuple");
        }
        if (value == null) {
            throw new NullPointerException();
        }
        return value.equals(tuple.getValue());
    }
}
