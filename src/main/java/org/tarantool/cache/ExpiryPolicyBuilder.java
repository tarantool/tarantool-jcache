/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tarantool.cache;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;

import java.util.Objects;

/**
 * Builder and utilities for getting predefined {@link ExpiryPolicy} instances.
 */
public final class ExpiryPolicyBuilder implements Factory<ExpiryPolicy>{

  /**
   * The serialVersionUID required for {@link java.io.Serializable}.
   */
  public static final long serialVersionUID = 201305101603L;

  /**
   * Get an {@link ExpiryPolicy} instance for a non expiring (i.e. "eternal") cache.
   *
   * @return the no expiry instance
   */
  public static ExpiryPolicy noExpiration() {
    return new ExpiryPolicy() {
        @Override
        public Duration getExpiryForCreation() {
          return Duration.ETERNAL;
        }

        @Override
        public Duration getExpiryForAccess() {
          return null;
        }

        @Override
        public Duration getExpiryForUpdate() {
          return null;
        }
      };
  }

  /**
   * Get a time-to-live (TTL) {@link ExpiryPolicy} instance for the given {@link Duration}.
   *
   * @param timeToLive the TTL duration
   * @return a TTL expiry
   */
  public static ExpiryPolicy timeToLiveExpiration(Duration timeToLive) {
    Objects.requireNonNull(timeToLive, "TTL duration cannot be null");
    return new TimeToLiveExpiryPolicy(timeToLive);
  }

  /**
   * Get a time-to-idle (TTI) {@link ExpiryPolicy} instance for the given {@link Duration}.
   *
   * @param timeToIdle the TTI duration
   * @return a TTI expiry
   */
  public static ExpiryPolicy timeToIdleExpiration(Duration timeToIdle) {
    Objects.requireNonNull(timeToIdle, "TTI duration cannot be null");
    return new TimeToIdleExpiryPolicy(timeToIdle);
  }

  /**
   * Fluent API for creating an {@link ExpiryPolicy} instance where you can specify constant values for creation, access and update time.
   * Unspecified values will be set to {@link ExpiryPolicy#INFINITE INFINITE} for create and {@code null} for access and update, matching
   * the {@link #noExpiration()}  no expiration} expiry.
   *
   * @return an {@link ExpiryPolicy} builder
   */
  public static ExpiryPolicyBuilder expiry() {
    return new ExpiryPolicyBuilder();
  }

  /**
  *
  * @return an {@link ExpiryPolicy}
  */
  @Override
  public ExpiryPolicy create() {
      return new BaseExpiryPolicy(create, access, update);
  }

  private Duration create = Duration.ETERNAL;
  private Duration access = null;
  private Duration update = null;

  private ExpiryPolicyBuilder() {}

  /**
   * Set TTL since creation
   *
   * @param create TTL since creation
   * @return this builder
   */
  public ExpiryPolicyBuilder setCreateDuration(Duration create) {
    Objects.requireNonNull(create, "Create duration cannot be null");
    this.create = create;
    return this;
  }

  /**
   * Set TTL since last access
   *
   * @param access TTL since last access
   * @return this builder
   */
  public ExpiryPolicyBuilder setAccessDuration(Duration access) {
    this.access = access;
    return this;
  }

  /**
   * Set TTL since last update
   *
   * @param update TTL since last update
   * @return this builder
   */
  public ExpiryPolicyBuilder setUpdateDuration(Duration update) {
    this.update = update;
    return this;
  }

  /**
   *
   * @return an {@link ExpiryPolicy}
   */
  public ExpiryPolicy build() {
    return new BaseExpiryPolicy(create, access, update);
  }

  /**
   * Simple implementation of the {@link ExpiryPolicy} interface allowing to set constants to each expiry types.
   */
  private static class BaseExpiryPolicy implements ExpiryPolicy {

    private final Duration create;
    private final Duration access;
    private final Duration update;

    /**
     * Constructs an {@link BaseExpiryPolicy} with the given
     * creation, access and update times.
     * 
     * @param create TTL since creation
     * @param access TTL since last access
     * @param update TTL since last update
     */
    BaseExpiryPolicy(Duration create, Duration access, Duration update) {
      this.create = create;
      this.access = access;
      this.update = update;
    }
    @Override
    public Duration getExpiryForCreation() {
      return create;
    }

    @Override
    public Duration getExpiryForAccess() {
      return access;
    }

    @Override
    public Duration getExpiryForUpdate() {
      return update;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final BaseExpiryPolicy that = (BaseExpiryPolicy) o;

      if (!Objects.equals(access, that.access)) return false;
      if (!Objects.equals(create, that.create)) return false;
      if (!Objects.equals(update, that.update)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = Objects.hashCode(create);
      result = 31 * result + Objects.hashCode(access);
      result = 31 * result + Objects.hashCode(update);
      return result;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + "{" +
             "create=" + create +
             ", access=" + access +
             ", update=" + update +
             '}';
    }

  }

  private static final class TimeToLiveExpiryPolicy extends BaseExpiryPolicy {
    TimeToLiveExpiryPolicy(Duration ttl) {
      super(ttl, null, ttl);
    }
  }

  private static final class TimeToIdleExpiryPolicy extends BaseExpiryPolicy {
    TimeToIdleExpiryPolicy(Duration tti) {
      super(tti, tti, tti);
    }
  }
}
