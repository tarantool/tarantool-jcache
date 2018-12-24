package org.tarantool.jsr107;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tarantool.cache.ExpiryTimeConverter;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

public class ExpiryPolicyConverter implements ExpiryTimeConverter {
    private static final Logger log = LoggerFactory.getLogger(ExpiryPolicyConverter.class);

    /**
     * The {@link ExpiryPolicy} defines expiration policy.
     */
    private final ExpiryPolicy expiryPolicy;

    /**
     * Constructs an {@link ExpiryPolicyConverter}.
     *
     * @param expiryPolicy {@link ExpiryPolicy}
     */
    public ExpiryPolicyConverter(ExpiryPolicy expiryPolicy) {
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public long getExpiryForCreation(long creationTime) {
        Duration duration;
        try {
            duration = expiryPolicy.getExpiryForCreation();
        } catch (Throwable t) {
            /* The default Duration to use when a Duration can't be determined */
            duration = Duration.ETERNAL;
        }
        return duration.getAdjustedTime(creationTime);
    }

    @Override
    public long getExpiryForAccess(long accessTime) {
        try {
            Duration duration = expiryPolicy.getExpiryForAccess();
            return duration.getAdjustedTime(accessTime);
        } catch (Throwable t) {
            //leave the expiry time untouched when we can't determine a duration
            log.error("Exception occurred during determination expire policy for Access", t);
        }
        return -1;
    }

    @Override
    public long getExpiryForUpdate(long updateTime) {
        try {
            Duration duration = expiryPolicy.getExpiryForUpdate();
            return duration.getAdjustedTime(updateTime);
        } catch (Throwable t) {
            //leave the expiry time untouched when we can't determine a duration
            log.error("Exception occurred during determination expire policy for Update", t);
        }
        return -1;
    }

}
