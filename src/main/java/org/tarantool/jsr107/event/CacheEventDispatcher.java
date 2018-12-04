/**
 *  Copyright 2011-2013 Terracotta, Inc.
 *  Copyright 2011-2013 Oracle America Incorporated
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
package org.tarantool.jsr107.event;

import static java.util.Collections.singletonList;


import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.Cache;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;

import javax.cache.event.EventType;
import java.util.ArrayList;
import java.util.List;

/**
 * Collects and appropriately dispatches {@link CacheEntryEvent}s to
 * {@link CacheEntryListener}s.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 * @author Brian Oliver
 * @author Evgeniy Zaikin
 */
public class CacheEventDispatcher<K, V> {

  /**
   * The List of {@link CacheEntryListenerRegistration} for the
   * {@link Cache}.
   */
  private final Iterable<CacheEntryListenerRegistration<K, V>> listenerRegistrations;

  /**
   * Constructs an {@link CacheEventDispatcher}.
   * @param registrations the {@link CacheEntryListenerRegistration}s defining
   *    {@link CacheEntryListener}s to which to dispatch events
   */
  public CacheEventDispatcher(Iterable<CacheEntryListenerRegistration<K, V>> registrations) {
      this.listenerRegistrations = registrations;
  }

  /**
   * Dispatches the event to the listeners
   *
   * @param event the event to be dispatched
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void dispatch(CacheEntryEvent<K, V> event) {

    //TODO: we could really optimize this implementation

    //TODO: we need to handle exceptions here

    //TODO: we need to work out which events should be raised synchronously or asynchronously

    //TODO: we need to remove/hide old values appropriately

    try {
        for (CacheEntryListenerRegistration<K, V> registration : listenerRegistrations) {
          Iterable<CacheEntryEvent<K, V>> iterable;
          CacheEntryEventFilter<? super K, ? super V> filter = registration.getCacheEntryFilter();
          if (filter != null) {
              iterable = new CacheEntryEventFilteringIterable<K, V>(singletonList(event), filter);
          } else {
              iterable = singletonList(event);
          }

          CacheEntryListener<? super K, ? super V> listener = registration.getCacheEntryListener();

          //notify expiry listeners
          if (listener instanceof CacheEntryExpiredListener && event.getEventType() == EventType.EXPIRED) {
            ((CacheEntryExpiredListener) listener).onExpired(cloneEvents(registration, iterable));
          }

          //notify create listeners
          if (listener instanceof CacheEntryCreatedListener && event.getEventType() == EventType.CREATED) {
            ((CacheEntryCreatedListener) listener).onCreated(cloneEvents(registration, iterable));
          }

          //notify update listeners
          if (listener instanceof CacheEntryUpdatedListener && event.getEventType() == EventType.UPDATED) {
            ((CacheEntryUpdatedListener) listener).onUpdated(cloneEvents(registration, iterable));
          }

          //notify remove listeners
          if (listener instanceof CacheEntryRemovedListener && event.getEventType() == EventType.REMOVED) {
            ((CacheEntryRemovedListener) listener).onRemoved(cloneEvents(registration, iterable));
          }
        }

    } catch (Exception e) {
      if (!(e instanceof CacheEntryListenerException)) {
        throw new CacheEntryListenerException("Exception on listener execution", e);
      } else {
        throw e;
      }
    }
  }

  private List<CacheEntryEvent<K, V>> cloneEvents(CacheEntryListenerRegistration<K, V> registration,
                                                  Iterable<CacheEntryEvent<K, V>> events) {
    List<CacheEntryEvent<K, V>> dispatchedEvents = new ArrayList<CacheEntryEvent<K, V>>();
    // clone events, setting or not the old value depending on registration properties
    for (CacheEntryEvent<K, V> event : events) {
      TarantoolCacheEntryEvent<K, V> dispatchedEvent;
      if (registration.isOldValueRequired() && event.getEventType() != EventType.CREATED) {
        dispatchedEvent = new TarantoolCacheEntryEvent<K, V>(event.getSource(), event.getKey(), event.getValue(), event.getOldValue(), event.getEventType());
      } else {

        if (event.getEventType() == EventType.REMOVED || event.getEventType() == EventType.EXPIRED) {

          if (registration.isOldValueRequired()) {

              dispatchedEvent = new TarantoolCacheEntryEvent<K, V>(event.getSource(), event.getKey(),
                          event.getValue(), event.getOldValue(), event.getEventType(), true);
          } else {

              dispatchedEvent = new TarantoolCacheEntryEvent<K, V>(event.getSource(), event.getKey(),
                          null, null, event.getEventType(), false);
          }
        } else {
          dispatchedEvent = new TarantoolCacheEntryEvent<K, V>(event.getSource(), event.getKey(), event.getValue(), null,
                  event.getEventType(), false);
        }

      }
      dispatchedEvents.add(dispatchedEvent);
    }
    return dispatchedEvents;
  }

}
