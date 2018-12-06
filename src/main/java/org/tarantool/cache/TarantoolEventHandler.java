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
     * Called after one or more entries have been expired by the cache. This is not
     * necessarily when an entry is expired, but when the cache detects the expiry.
     *
     * @param key of the entry just removed
     * @param newValue the newValue
     * @param oldValue the oldValue
     * @throws CacheEntryListenerException if there is problem executing the listener
     */
    void onExpired(K key, V newValue, V oldValue);

    /**
     * Called after one or more entries have been created.
     * @param key of the entry just created
     * @param newValue the newValue
     * @param oldValue the oldValue
     * @throws CacheEntryListenerException if there is problem executing the listener
     */
    void onCreated(K key, V newValue, V oldValue);

    /**
     * Called after one or more entries have been updated.
     *
     * @param key of the entry just updated
     * @param newValue the newValue
     * @param oldValue the oldValue
     * @throws CacheEntryListenerException if there is problem executing the listener
     */
    void onUpdated(K key, V newValue, V oldValue);

    /**
     * Called after one or more entries have been removed. If no entry existed for
     * a key an event is not raised for it.
     *
     * @param key of the entry just removed
     * @param newValue the newValue
     * @param oldValue the oldValue
     * @throws CacheEntryListenerException if there is problem executing the listener
     */
    void onRemoved(K key, V newValue, V oldValue);

}
