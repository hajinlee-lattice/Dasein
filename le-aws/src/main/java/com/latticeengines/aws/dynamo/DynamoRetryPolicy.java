package com.latticeengines.aws.dynamo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.classify.Classifier;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

/**
 * Decorator policy that has knowledge about dynamodb specific exceptions
 */
public class DynamoRetryPolicy extends ExceptionClassifierRetryPolicy {
    private static final long serialVersionUID = -7921729917576002508L;

    private static final Map<Class<? extends Throwable>, Boolean> RETRY_EXCEPTIONS = new HashMap<>();
    private static final Set<String> THROTTLING_ERROR_CODES = new HashSet<>();
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
        THROTTLING_ERROR_CODES.add("ThrottlingException"); // real one
        THROTTLING_ERROR_CODES.add("ThrottlingError"); // the one in their comment
        THROTTLING_ERROR_CODES.add("ThrottlingErrorException"); // just in case
        THROTTLING_ERROR_CODES.add("ProvisionedThroughputExceeded"); // the one in their comment
        THROTTLING_ERROR_CODES.add("ProvisionedThroughputExceededException"); // real one
        THROTTLING_ERROR_CODES.forEach(code -> RETRY_ERROR_CODES.put(code, true));
    }

    private final RetryPolicy policy;
    private final Classifier<Throwable, RetryPolicy> classifier;

    public DynamoRetryPolicy(@NotNull RetryPolicy policy) {
        Preconditions.checkNotNull(policy);
        this.policy = policy;
        classifier = (exception) -> {
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
        };
        setExceptionClassifier(classifier);
    }

    @VisibleForTesting
    Classifier<Throwable, RetryPolicy> getClassifier() {
        return classifier;
    }

    private RetryPolicy checkRetryExceptionMap(Throwable exception) {
        return RETRY_EXCEPTIONS.entrySet().stream() //
                .filter(entry -> ExceptionUtils.indexOfType(exception, entry.getKey()) != -1) //
                .map(Map.Entry::getValue) //
                .map(shouldRetry -> shouldRetry ? policy : NEVER_RETRY) //
                .findFirst() //
                .orElse(null);
    }

    public static boolean isThrottlingError(Throwable e) {
        if (e == null) {
            return false;
        } else if (ExceptionUtils.indexOfType(e, ProvisionedThroughputExceededException.class) != -1) {
            return true;
        } else if (e instanceof AmazonDynamoDBException) {
            // check error code in base class
            AmazonDynamoDBException dynamoException = (AmazonDynamoDBException) e;
            String errorCode = dynamoException.getErrorCode();
            if (THROTTLING_ERROR_CODES.contains(errorCode)) {
                return true;
            }
        }
        return false;
    }
}
