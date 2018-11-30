package org.tarantool.cache;

import static java.util.Collections.singletonList;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tarantool.Iterator;

public class TarantoolSpace {

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

    private static final String DEFAULT_INDEX_TYPE = "tree";

    private static final int MAX_ROWS_PER_ITER_ALL = 65535;

    /**
     * Execute and evaluate.
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
            Map<String, Map<?,?>> index = (Map<String, Map<?,?>>) response.get(0);
            Map<?,?> primary = index.get("primary");
            if (primary != null) {
                Object indexType = primary.get("type");
                if (indexType != null && indexType.toString().equalsIgnoreCase(DEFAULT_INDEX_TYPE)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Constructs an Space representation.
     *
     * @param session       the TarantoolSession that's creating this representation
     * @param cacheName     the name of the Cache
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
     * @param keys List<?> keys
     * @return List<?> as response.
     * @throws TarantoolException if keys list is empty
     */
    public List<?> select(List<?> keys) {
      try {
          return session.syncOps().select(spaceId, 0, keys, 0, 1, Iterator.EQ .ordinal());
      } catch (Exception e) {
          throw new TarantoolCacheException(e);
      }
    }

    /**
     * Select all available tuples in this space
     * @param keys List<?> keys
     * @return List<?> as response.
     */
    public List<?> select() {
      try {
          // Adjust max size of batch per select
          int limit = MAX_ROWS_PER_ITER_ALL;
          return session.syncOps().select(spaceId, 0, Collections.emptyList(), 0, limit, Iterator.ALL.ordinal());
      } catch (Exception e) {
          throw new TarantoolCacheException(e);
      }
    }

    /**
     * Fetch next tuple by executing "select" request.
     * Uses Iterator.GT iterator to get next tuple.
     * @param keys List<?> current tuple keys
     * @return List<?> as response.
     * @throws TarantoolException if keys list is empty
     */
    public List<?> next(List<?> keys) {
      try {
          /**
           * Adjust iterator for fetching next tuple from Tarantool's space
           * Tarantool supports different type of iteration (See TarantoolIterator),
           * but not every index (HASH, TREE, ...) supports these types.
           * See https://tarantool.io/en/doc/2.0/book/box/data_model/
           */
          return session.syncOps().select(spaceId, 0, keys, 0, 1, Iterator.GT.ordinal());
      } catch (Exception e) {
          throw new TarantoolCacheException(e);
      }
    }

    /**
     * Fetch first available tuple in this space
     * @param keys List<?> keys
     * @return List<?> as response.
     */
    public List<?> first() {
      try {
          /* Limit is always 1 to fetch only one tuple */
          return session.syncOps().select(spaceId, 0, Collections.emptyList(), 0, 1, Iterator.ALL.ordinal());
      } catch (Exception e) {
          throw new TarantoolCacheException(e);
      }
    }

    /**
     * Execute insert request.
     * @param tuple List<?> tuple to insert
     * @return List<?> list of inserted tuples, or empty list if failed.
     */
    public List<?> insert(List<?> tuple) {
      try {
          return session.syncOps().insert(spaceId, tuple);
      } catch (Exception e) {
          throw new TarantoolCacheException(e);
      }
    }

    /**
     * Execute update request.
     * @param keys List<?> keys
     * @param Object... ops operations for update
     * @return List<?> list of updated tuples.
     */
    public List<?> update(List<?> keys, Object... ops) {
      try {
          return session.syncOps().update(spaceId, keys, ops);
      } catch (Exception e) {
          throw new TarantoolCacheException(e);
      }
    }

    /**
     * Execute "update or insert" request.
     * @param keys List<?> keys
     * @param defTuple List<?> tuple to insert (if not exists yet)
     * @param Object... ops operations for update (if tuple exists)
     */
    public List<?> upsert(List<?> keys, List<?> defTuple, Object... ops) {
      try {
          return session.syncOps().upsert(spaceId, keys, defTuple, ops);
      } catch (Exception e) {
          throw new TarantoolCacheException(e);
      }
    }

    /**
     * Execute "delete" request.
     * @param keys List<?> keys
     * @return List<?> as list of actually deleted tuples.
     */
    public List<?> delete(List<?> keys) {
      try {
          return session.syncOps().delete(spaceId, keys);
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

}
