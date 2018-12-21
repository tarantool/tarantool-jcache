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

import java.util.Collection;
import java.util.Map;

/**
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Evgeniy Zaikin
 */
public interface CacheStore<K, V> {
    /**
     * Loads an object. Application developers should implement this
     * method to customize the loading of a value for a cache entry. This method
     * is called by a cache when a requested entry is not in the cache. If
     * the object can't be loaded <code>null</code> should be returned.
     *
     * @param key the key identifying the object being loaded
     * @return The value for the entry that is to be stored in the cache or
     * <code>null</code> if the object can't be loaded
     */
    public V load(K key);

    /**
     * Writes the Cache Entry to the configured CacheWriter.
     *
     * @param key   of the Cache Entry to write
     * @param value of the Cache Entry to write
     */
    public void write(K key, V value);

    /**
     * Write the specified entries to the external resource. This method is intended
     * to support both insert and update.
     * If this operation fails (by throwing an exception) after a partial success,
     * the writer must remove any successfully written entries from the entries
     * collection so that the caching implementation knows what succeeded and can
     * mutate the cache.
     *
     * @param map of entries to write. Upon invocation, it contains
     *                the entries to write for write-through. Upon return the
     *                collection must only contain entries that were not
     *                successfully written. (see partial success above)
     */
    public void writeAll(Map<? extends K, ? extends V> map);

    /**
     * Deletes the Cache Entry using the configured CacheWriter.
     *
     * @param key of the Cache Entry to delete
     */
    public void delete(K key);

    /**
     * Remove data and keys from the external resource for the given collection of
     * keys, if present.
     * <p>
     * The order that individual deletes occur is undefined.
     * <p>
     * If this operation fails (by throwing an exception) after a partial success,
     * the writer must remove any successfully written entries from the entries
     * collection so that the caching implementation knows what succeeded and can
     * mutate the cache.
     * <p>
     * Expiry of a cache entry is not a delete hence will not cause this method to
     * be invoked.
     * <p>
     * This method may include keys even if there is no mapping for that key,
     * in which case the data represented by that key should be removed from the
     * underlying resource.
     *
     * @param keys a mutable collection of keys for entries to delete. Upon
     *             invocation, it contains the keys to delete for write-through.
     *             Upon return the collection must only contain the keys that were
     *             not successfully deleted. (see partial success above)
     */
    public void deleteAll(Collection<?> keys);
}
