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

import static java.util.Collections.singletonList;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TarantoolTuple<K,V> extends AbstractList<Object> {

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
    private final TarantoolSpace<K,V> space;

    /**
     * Constructs an {@link TarantoolTuple}
     *
     * @param space {@link TarantoolSpace} where {@link TarantoolTuple} is stored.
     */
    public TarantoolTuple(TarantoolSpace<K,V> space) {
        this.space = space;
        for (int i = 0; i < updateOperations.length; i++) {
            updateOperations[i][0] = "=";
            updateOperations[i][1] = i;
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
        return (K)values[0];
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
        return (V)values[1];
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
    public long getExpiryTime() {
        return Long.class.cast(values[2]);
    }

    /**
     * Sets the time (since the Epoch) in milliseconds when the tuple
     * associated with this value should be considered expired.
     *
     * @param expiryTime time in milliseconds (since the Epoch)
     */
    public void setExpiryTime(long expiryTime) {
        values[2] = updateOperations[2][2] = expiryTime;
    }

    /**
     * Determines if the Cache Entry associated with this value would be expired
     * at the specified time
     *
     * @param now time in milliseconds (since the Epoch)
     * @return true if the value would be expired at the specified time
     * @throws IllegalStateException if cursor is not opened
     */
    public boolean isExpiredAt(long now) {
      long expiryTime = getExpiryTime();
      return expiryTime > -1 && expiryTime <= now;
    }

    /**
     * Invalidate all the values.
     */
    public void invalidate() {
        for (int i = 0; i < values.length; i++) {
            values[i] = null;
        }
    }

    /**
     * Puts given values to the current {@link TarantoolTuple}.
     *
     * @param values to be put
     * @throws TarantoolCacheException if incorrect tuple was selected from space
     */
    public void assign(List<?> values) {
        /* Check here values size, it must be same (key, value, expiryTime, sessionId, lock) */
        if (values.size() == this.size()) {
            for (int i = 0; i < this.size(); i++) {
                updateOperations[i][2] = this.values[i] = values.get(i);
            }
        } else {
            throw new TarantoolCacheException("Incorrect tuple given");
        }
    }

    /**
     * Performs the update value operation.
     */
    public void updateValue() {
        space.update(singletonList(getKey()), (Object) updateOperations[1]);
    }

    /**
     * Performs the update expiryTime operation.
     */
    public void updateExpiry() {
        space.update(singletonList(getKey()), (Object) updateOperations[2]);
    }

    /**
     * Performs the full update operation (value and expiryTime).
     */
    public void update() {
        Object[] ops = {updateOperations[1], updateOperations[2]};
        space.update(singletonList(getKey()), ops);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      return getKey().hashCode();
    }
}
