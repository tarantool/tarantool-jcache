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

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * TarantoolTuple is friendly implementation of {@link List}.
 * Implements index-based getters to be used outside.
 * Provides easy-used methods to get and set field to appropriate position.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Evgeniy Zaikin
 */
public class TarantoolTuple<K, V> extends AbstractList<Object> {

    /**
     * Field count
     */
    private static final int FIELD_COUNT = 5;

    /**
     * This array stores values for all the fields
     */
    private final Object[] values = new Object[FIELD_COUNT];

    /**
     * This array holds an Update operations, every update operation looks like {"=", i, newValue},
     * where "=" means replace operation, i - number of field in tuple (0 .. FIELD_COUNT)
     * That's why here we have two-dimensional array (array of array)
     */
    private final Object[][] updateOperations = new Object[FIELD_COUNT][3];

    /**
     * The {@link TarantoolSpace} is space where {@link TarantoolTuple} is stored.
     */
    private final TarantoolSpace<K, V> space;

    /**
     * Constructs an {@link TarantoolTuple}
     *
     * @param space {@link TarantoolSpace} where {@link TarantoolTuple} is stored.
     * @throws NullPointerException if a given space is null
     */
    TarantoolTuple(TarantoolSpace<K, V> space) {
        if (space == null) {
            throw new NullPointerException();
        }
        this.space = space;
        for (int i = 0; i < updateOperations.length; i++) {
            updateOperations[i][0] = "=";
            updateOperations[i][1] = i;
        }
    }

    /**
     * Constructs an {@link TarantoolTuple}
     *
     * @param space      {@link TarantoolSpace} where {@link TarantoolTuple} is stored.
     * @param key        the key
     * @param value      the value
     * @param expiryTime time in milliseconds (since the Epoch)
     * @throws NullPointerException if a given space is null
     */
    TarantoolTuple(TarantoolSpace<K, V> space, K key, V value, long expiryTime) {
        this(space);
        setKey(key);
        setValue(value);
        setExpiryTime(expiryTime);
    }

    /**
     * Constructs an {@link TarantoolTuple}
     *
     * @param space {@link TarantoolSpace} where {@link TarantoolTuple} is stored.
     * @param key   the key
     * @throws NullPointerException if a given space is null
     * @throws NullPointerException if a given key is null
     */
    TarantoolTuple(TarantoolSpace<K, V> space, K key) {
        this(space);
        if (key == null) {
            throw new NullPointerException();
        }
        setKey(key);
    }

    /**
     * Constructs an {@link TarantoolTuple} and sets values from given
     *
     * @param space  {@link TarantoolSpace} where {@link TarantoolTuple} is stored.
     * @param values to be put
     * @throws NullPointerException    if a given space is null
     * @throws TarantoolCacheException if incorrect tuple was selected from space
     */
    TarantoolTuple(TarantoolSpace<K, V> space, List<?> values) {
        this(space);
        /* Check here values size, it must be same (key, value, expiryTime, sessionId, lock) */
        if (values.size() == this.size()) {
            for (int i = 0; i < this.size(); i++) {
                updateOperations[i][2] = this.values[i] = values.get(i);
            }
        } else {
            throw new TarantoolCacheException("Incorrect tuple given");
        }
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Object[] toArray() {
        return values.clone();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        int size = size();
        if (a.length < size)
            return Arrays.copyOf(this.values, size,
                    (Class<? extends T[]>) a.getClass());
        System.arraycopy(this.values, 0, a, 0, size);
        if (a.length > size)
            a[size] = null;
        return a;
    }

    @Override
    public Object get(int index) {
        return values[index];
    }

    @Override
    public void sort(Comparator<? super Object> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the key corresponding to this tuple.
     *
     * @return the key corresponding to this tuple
     */
    @SuppressWarnings("unchecked")
    public K getKey() {
        return (K) values[0];
    }

    /**
     * Sets the key to tuple
     *
     * @param key to be put
     */
    public void setKey(K key) {
        values[0] = updateOperations[0][2] = key;
    }

    /**
     * Returns the value corresponding to this tuple.
     *
     * @return the value corresponding to this tuple
     */
    @SuppressWarnings("unchecked")
    public V getValue() {
        return (V) values[1];
    }

    /**
     * Sets the value to tuple
     *
     * @param value the Value
     */
    public void setValue(V value) {
        values[1] = updateOperations[1][2] = value;
    }

    /**
     * Gets the time (since the Epoch) in milliseconds when the tuple
     * associated with this value should be considered expired.
     *
     * @return expiry time in milliseconds (since the Epoch)
     */
    long getExpiryTime() {
        return Long.class.cast(values[2]);
    }

    /**
     * Sets the time (since the Epoch) in milliseconds when the tuple
     * associated with this value should be considered expired.
     *
     * @param expiryTime time in milliseconds (since the Epoch)
     */
    void setExpiryTime(long expiryTime) {
        values[2] = updateOperations[2][2] = expiryTime;
    }

    /**
     * Determines if the Cache Entry associated with this value would be expired
     * at the specified time
     *
     * @param now time in milliseconds (since the Epoch)
     * @return true if the value would be expired at the specified time
     */
    boolean isExpiredAt(long now) {
        long expiryTime = getExpiryTime();
        return expiryTime > -1 && expiryTime <= now;
    }

    /**
     * Performs the update value operation.
     *
     * @param value the Value
     */
    void updateValue(V value) {
        values[1] = updateOperations[1][2] = value;
        space.update(getKey(), (Object) updateOperations[1]);
    }

    /**
     * Sets the time (since the Epoch) in milliseconds when the tuple
     * associated with this value should be considered expired.
     *
     * @param expiryTime time in milliseconds (since the Epoch)
     */
    void updateExpiry(long expiryTime) {
        values[2] = updateOperations[2][2] = expiryTime;
        space.update(getKey(), (Object) updateOperations[2]);
    }

    /**
     * Locks the tuple.
     *
     * @return locked tuple if succeeded, {@code null} otherwise
     */
    TarantoolTuple<K, V> tryLock() {
        values[3] = updateOperations[3][2] = true;
        List<?> response = space.update(getKey(), (Object) updateOperations[3]);
        if (response.isEmpty()) {
            return null;
        }
        return new TarantoolTuple<>(space, (List<?>) response.iterator().next());
    }

    /**
     * Tries to insert and lock the tuple.
     *
     * @return true if succeeded
     */
    boolean tryPush() {
        values[3] = updateOperations[3][2] = true;
        try {
            space.insert(this);
            return true;
        } catch (TarantoolCacheException e) {
            return false;
        }
    }

    /**
     * Performs the full update operation (value and expiryTime).
     *
     * @param value      the Value
     * @param expiryTime time in milliseconds (since the Epoch)
     */
    void update(V value, long expiryTime) {
        values[1] = updateOperations[1][2] = value;
        values[2] = updateOperations[2][2] = expiryTime;
        values[3] = updateOperations[3][2] = null;
        space.replace(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return getKey().hashCode();
    }
}
