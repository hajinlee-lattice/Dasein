package com.latticeengines.aws.dynamo;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.ExceptionClassifierRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;

import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ItemCollectionSizeLimitExceededException;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.amazonaws.services.dynamodbv2.model.TransactionConflictException;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

/**
 * Decorator policy that has knowledge about dynamodb specific exceptions
 */
public class DynamoRetryPolicy extends ExceptionClassifierRetryPolicy {
    private static final long serialVersionUID = -7921729917576002508L;

    private static final Map<Class<? extends Throwable>, Boolean> RETRY_EXCEPTIONS = new HashMap<>();
    private static final Map<String, Boolean> RETRY_ERROR_CODES = new HashMap<>();

    private static final RetryPolicy NEVER_RETRY = new NeverRetryPolicy();
    static {
        // instantiate retry exception map

        // exceptions that can be retried
        RETRY_EXCEPTIONS.put(LimitExceededException.class, true);
        RETRY_EXCEPTIONS.put(ProvisionedThroughputExceededException.class, true);
        RETRY_EXCEPTIONS.put(ItemCollectionSizeLimitExceededException.class, true);
        RETRY_EXCEPTIONS.put(TransactionConflictException.class, true);
        RETRY_EXCEPTIONS.put(InternalServerErrorException.class, true);
        // exceptions that cannot be retried
        RETRY_EXCEPTIONS.put(ResourceInUseException.class, false);
        RETRY_EXCEPTIONS.put(ResourceNotFoundException.class, false);
        RETRY_EXCEPTIONS.put(ConditionalCheckFailedException.class, false);
        RETRY_EXCEPTIONS.put(TransactionCanceledException.class, false);

        /*-
         * some error don't have explicit exception instance, just have specific error code
         * 1. ThrottlingException: when on-demand table is scaling up
         * 2. ProvisionedThroughputExceeded: just in case, or if we decide to use dynamo transaction later
         */
        RETRY_ERROR_CODES.put("ThrottlingException", true); // real one
        RETRY_ERROR_CODES.put("ThrottlingError", true); // the one in their comment
        RETRY_ERROR_CODES.put("ThrottlingErrorException", true); // just in case
        RETRY_ERROR_CODES.put("ProvisionedThroughputExceeded", true); // the one in their comment
        RETRY_ERROR_CODES.put("ProvisionedThroughputExceededException", true); // real one
    }

    private final RetryPolicy policy;

    public DynamoRetryPolicy(@NotNull RetryPolicy policy) {
        Preconditions.checkNotNull(policy);
        this.policy = policy;
        setExceptionClassifier((exception) -> {
            // try exception class first
            RetryPolicy resultPolicy = checkRetryExceptionMap(exception);
            if (resultPolicy != null) {
                return resultPolicy;
            }

            // check error code in base class
            if (exception instanceof AmazonDynamoDBException) {
                // check error code
                AmazonDynamoDBException dynamoException = (AmazonDynamoDBException) exception;
                String errorCode = dynamoException.getErrorCode();
                if (RETRY_ERROR_CODES.containsKey(errorCode)) {
                    return Boolean.TRUE.equals(RETRY_ERROR_CODES.get(errorCode)) ? policy : NEVER_RETRY;
                }
            }

            return NEVER_RETRY;
        });
    }

    private RetryPolicy checkRetryExceptionMap(Throwable exception) {
        return RETRY_EXCEPTIONS.entrySet().stream() //
                .filter(entry -> ExceptionUtils.indexOfType(exception, entry.getKey()) != -1) //
                .map(Map.Entry::getValue) //
                .map(shouldRetry -> shouldRetry ? policy : NEVER_RETRY) //
                .findFirst() //
                .orElse(null);
    }
}
