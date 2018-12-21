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

import static java.util.Collections.singletonList;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tarantool.TarantoolException;

/**
 * TarantoolSpace associated with Tarantool's {space_object},
 * (See 'box.space.{space_object_name}' in Tarantool's help)
 * <p>
 * For every newly created {space_object} key field and hash index for this key field are built.
 * <p>
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Evgeniy Zaikin
 */
public class TarantoolSpace<K, V> implements Iterable<TarantoolTuple<K, V>> {

    private static final Logger log = LoggerFactory.getLogger(TarantoolSpace.class);

    /**
     * The {@link TarantoolSession} related to {@code cacheManager}
     */
    private final TarantoolSession session;

    /**
     * The ID of the current space in Tarantool
     */
    private final Integer spaceId;

    /**
     * The name of the current space in Tarantool
     */
    private final String spaceName;

    /**
     * The type of the index to be used in the Tarantool's spaces.
     * Hash indexes have some problems when accessing tuple with Iterator.GT,
     * so Tree index is most appropriate for now.
     */
    private static final String DEFAULT_INDEX_TYPE = "tree";

    /**
     * Execute and evaluate.
     *
     * @param expression string
     * @return List<?> as response.
     */
    private List<?> execute(String expression) {
        try {
            return session.syncOps().eval("return " + expression);
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    private int getSpaceId() {
        String command = "box.space." + spaceName + ".id";
        List<?> response = execute(command);
        if (response.isEmpty() || !Integer.class.isInstance(response.get(0))) {
            throw new TarantoolCacheException("Invalid response on command '" + command + "': expected integer got " + response);
        }
        return (Integer) response.get(0);
    }

    private int createSpace() {
        final String command = "box.schema.space.create('" + spaceName + "').id";
        final List<?> response = execute(command);
        if (response.isEmpty() || !Integer.class.isInstance(response.get(0))) {
            throw new TarantoolCacheException("Invalid response on command '" + command + "': expected integer got " + response);
        }
        final Integer spaceId = (Integer) response.get(0);

        try {
            final Map<String, String> fields = new HashMap<>();
            fields.put("name", "key");
            fields.put("type", "scalar");
            session.syncOps().call("box.space." + spaceName + ":format", singletonList(fields));
        } catch (Throwable t) {
            this.drop();
            throw new TarantoolCacheException("Cannot format space " + this.spaceName, t);
        }

        try {
            final Map<String, Object> index = new HashMap<>();
            index.put("parts", singletonList("key"));
            index.put("type", DEFAULT_INDEX_TYPE);
            session.syncOps().call("box.space." + spaceName + ":create_index", "primary", index);
        } catch (Throwable t) {
            this.drop();
            throw new TarantoolCacheException("Cannot create primary index in space " + this.spaceName, t);
        }

        return spaceId;
    }

    private boolean checkSpaceExists() {
        String command = "box.space." + spaceName + " ~= nil";
        List<?> response = execute(command);
        if (response.isEmpty() || !Boolean.class.isInstance(response.get(0))) {
            throw new TarantoolCacheException("Invalid response on command '" + command + "': expected boolean got " + response);
        }
        return (Boolean) response.get(0);
    }

    private boolean checkSpaceFormat() {
        String command = "box.space." + spaceName + ".index";
        List<?> response = execute(command);
        if (!response.isEmpty()) {
            @SuppressWarnings("unchecked")
            Map<String, Map<?, ?>> index = (Map<String, Map<?, ?>>) response.get(0);
            Map<?, ?> primary = index.get("primary");
            if (primary != null) {
                Object indexType = primary.get("type");
                return indexType != null && indexType.toString().equalsIgnoreCase(DEFAULT_INDEX_TYPE);
            }
        }
        return false;
    }

    protected void setSpaceTrigger(String triggerType, String triggerFunc) {
        String function = "box.space." + spaceName + ":" + triggerType;
        try {
            try {
                session.syncOps().call(function);
            } catch (TarantoolException e) {
                return;
            }
            session.syncOps().eval(function + "(" + triggerFunc + ")");
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Constructs an Space representation.
     *
     * @param session   the TarantoolSession that's creating this representation
     * @param cacheName the name of the Cache
     */
    public TarantoolSpace(TarantoolSession session,
                          String cacheName) {
        if (session == null || cacheName == null) {
            throw new NullPointerException();
        }
        this.session = session;
        this.spaceName = cacheName.replaceAll("[^a-zA-Z0-9]", "_");
        final boolean spaceExists = checkSpaceExists();
        if (spaceExists) {
            final boolean spaceFormatIsCorrect = checkSpaceFormat();
            if (spaceFormatIsCorrect) {
                // Space format is OK, we're able to use it
                this.spaceId = getSpaceId();
            } else {
                // Drops the space with incorrect format
                this.drop();
                // Creates a new space with appropriate format
                this.spaceId = createSpace();
            }
        } else {
            this.spaceId = createSpace();
        }

        log.info("cache initialized: spaceName={}, spaceId={}", spaceName, spaceId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getClass().getName() + "@" + spaceName;
    }

    /**
     * Execute "select" request.
     *
     * @param key the key
     * @return List<?> as response.
     */
    public List<?> select(K key) {
        try {
            int iter = org.tarantool.Iterator.EQ.getValue();
            return session.syncOps().select(spaceId, 0, singletonList(key), 0, 1, iter);
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Select all available tuples in this space
     *
     * @return List<?> as response.
     */
    public List<?> select() {
        try {
            int iter = org.tarantool.Iterator.ALL.getValue();
            // Adjust max size of batch per select
            int limit = Integer.MAX_VALUE;
            return session.syncOps().select(spaceId, 0, Collections.emptyList(), 0, limit, iter);
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Fetch next tuple by executing "select" request.
     * Uses Iterator.GT iterator to get next tuple.
     *
     * @param key current tuple key
     * @return List<?> as response.
     */
    public List<?> next(K key) {
        try {
            /*
             * Adjust iterator for fetching next tuple from Tarantool's space
             * Tarantool supports different type of iteration (See TarantoolIterator),
             * but not every index (HASH, TREE, ...) supports these types.
             * See https://tarantool.io/en/doc/2.0/book/box/data_model/
             */
            int iter = org.tarantool.Iterator.GT.getValue();
            return session.syncOps().select(spaceId, 0, singletonList(key), 0, 1, iter);
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Fetch first available tuple in this space
     *
     * @return List<?> as response.
     */
    public List<?> first() {
        try {
            int iter = org.tarantool.Iterator.ALL.getValue();
            /* Limit is always 1 to fetch only one tuple */
            return session.syncOps().select(spaceId, 0, Collections.emptyList(), 0, 1, iter);
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Execute insert request.
     *
     * @param tuple List<?> tuple to insert
     * @return List<?> list of inserted tuples, or empty list if failed.
     */
    public List<?> insert(TarantoolTuple<K, V> tuple) {
        try {
            return session.syncOps().insert(spaceId, tuple);
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Execute update request.
     *
     * @param key       the key
     * @param ops operations for update
     * @return List<?>  list of updated tuples.
     */
    public List<?> update(K key, Object... ops) {
        try {
            return session.syncOps().update(spaceId, singletonList(key), ops);
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Execute "update or insert" request.
     *
     * @param key       the key
     * @param defTuple  TarantoolTuple<K, V> tuple to insert (if not exists yet)
     * @param ops operations for update (if tuple exists)
     */
    public List<?> upsert(K key, TarantoolTuple<K, V> defTuple, Object... ops) {
        try {
            return session.syncOps().upsert(spaceId, singletonList(key), defTuple, ops);
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Execute "delete" request.
     *
     * @param key List<?> keys
     * @return List<?> as list of actually deleted tuples.
     */
    public List<?> delete(K key) {
        try {
            return session.syncOps().delete(spaceId, singletonList(key));
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Truncates (clears) this space
     */
    public void truncate() {
        try {
            session.syncOps().call("box.space." + this.spaceName + ":truncate");
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * Drops this space
     */
    public void drop() {
        try {
            session.syncOps().call("box.space." + this.spaceName + ":drop");
        } catch (Exception e) {
            throw new TarantoolCacheException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<TarantoolTuple<K, V>> iterator() {
        final Iterator<?> iterator = select().iterator();
        final TarantoolTuple<K, V> tuple = new TarantoolTuple<K, V>(this);
        /*
         * Select all Tarantool's tuples from Space, build iterator wrapper
         * for iterating over selected tuples.
         * Note: only ReadOnly mode is available, remove() is not supported.
         */
        return new Iterator<TarantoolTuple<K, V>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public TarantoolTuple<K, V> next() {
                tuple.assign((List<?>) iterator.next());
                return tuple;
            }
        };
    }

}
