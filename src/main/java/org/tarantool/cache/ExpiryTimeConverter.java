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
 * Defines functions to determine when cache entries will expire based on
 * creation, access and modification operations.
 * <p>
 * Each of the functions return a new time that is adjusted to the given current time.
 * A new time specifies the moment of time,
 * when a cache entry is considered expired.
 *
 * @author Evgeniy Zaikin
 */
public interface ExpiryTimeConverter {

    /**
     * Gets the time when a newly created entry is considered expired.
     *
     * @return the moment of time when a created entry expires
     */
    long getExpiryForCreation(long creationTime);

    /**
     * Gets the time when an accessed entry is considered expired.
     *
     * @return the moment of time when an accessed entry expires
     */
    long getExpiryForAccess(long accessTime);

    /**
     * Gets the time when an updated entry is considered expired.
     *
     * @return the moment of time when an updated entry expires
     */
    long getExpiryForUpdate(long updateTime);
}
