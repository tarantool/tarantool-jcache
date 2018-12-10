/**
 *  Copyright 2018 Evgeniy Zaikin
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
package org.tarantool.cache;

/**
 * Invoked after a cache entry is created, updated, removed, expired,
 * or if a batch call is made.
 * <p>
 * @param <K> the type of key
 * @param <V> the type of value
 * @author Evgeniy Zaikin
 */
public interface TarantoolEventHandler<K, V> {

    /**
     * Called after entry have been expired by the cache. This is not
     * necessarily when an entry is expired, but when the cache detects the expiry.
     *
     * @param key of the entry just removed
     * @param newValue the newValue
     * @param oldValue the oldValue
     * @throws CacheEntryListenerException if there is problem executing the listener
     */
    default void onExpired(K key, V newValue, V oldValue) {
    }

    /**
     * Called after entry have been created.
     * @param key of the entry just created
     * @param newValue the newValue
     * @param oldValue the oldValue
     * @throws CacheEntryListenerException if there is problem executing the listener
     */
    default void onCreated(K key, V newValue, V oldValue) {
    }

    /**
     * Called after entry have been updated.
     *
     * @param key of the entry just updated
     * @param newValue the newValue
     * @param oldValue the oldValue
     * @throws CacheEntryListenerException if there is problem executing the listener
     */
    default void onUpdated(K key, V newValue, V oldValue) {
    }

    /**
     * Called after entry have been removed. If no entry existed for
     * a key an event is not raised for it.
     *
     * @param key of the entry just removed
     * @param newValue the newValue
     * @param oldValue the oldValue
     * @throws CacheEntryListenerException if there is problem executing the listener
     */
    default void onRemoved(K key, V newValue, V oldValue) {
    }

}
