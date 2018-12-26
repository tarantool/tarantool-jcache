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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An {@link Iterator} over {@link TarantoolTuple}s that lazily selects
 * tuple from {@link TarantoolSpace}, updates expire time for access.
 * If tuple is expired - deletes it.
 * Used as server side cursor.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Evgeniy Zaikin
 */
public class TarantoolCursor<K, V> implements Iterator<TarantoolTuple<K, V>> {

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
     * The time the iteration commenced.  We use this to determine what
     * Cache Entries in the underlying iterator are expired.
     */
    private final long now = System.currentTimeMillis();

    /**
     * The internal {@link Iterator}.
     */
    private Iterator<?> iterator;

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
    TarantoolCursor(TarantoolSpace<K, V> space,
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
        iterator = space.first().iterator();
    }

    /**
     * Delete expired Tarantool's tuple.
     * Stores fields of the deleted tuple to internal structure.
     * Calls appropriate event.
     */
    private void expire() {
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
     */
    private void access(long accessTime) {
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

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public TarantoolTuple<K, V> next() {
        if (iterator.hasNext()) {
            // Construct tuple
            tuple = new TarantoolTuple<>(space, (List<?>) iterator.next());
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
        if (tuple == null) {
            throw new IllegalStateException("Must progress to the next entry to remove");
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
        tuple = null;
    }
}
