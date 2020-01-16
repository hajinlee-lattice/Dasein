package com.latticeengines.aws.dynamo;

import org.springframework.retry.RetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class DynamoRetryPolicyUnitTestNG {

    @Test(groups = "unit", dataProvider = "dynamoRetryClassifier")
    private void testDynamoRetryClassifier(Throwable e, boolean shouldRetry) {
        SimpleRetryPolicy policy = new SimpleRetryPolicy(3);

        DynamoRetryPolicy dynamoRetryPolicy = new DynamoRetryPolicy(policy);
        RetryPolicy result = dynamoRetryPolicy.getClassifier().classify(e);
        Assert.assertNotNull(result);
        if (shouldRetry) {
            Assert.assertEquals(result, policy);
        } else {
            Assert.assertTrue(result instanceof NeverRetryPolicy);
        }
    }

    @Test(groups = "unit", dataProvider = "dynamoThrottlingException")
    private void testIsDynamoThrottlingException(Throwable e, boolean isThrottlingException) {
        Assert.assertEquals(DynamoRetryPolicy.isThrottlingError(e), isThrottlingException, String.format(
                "Exception %s is classified wrong. isThrottlingException = %b", e.toString(), isThrottlingException));
    }

    @DataProvider(name = "dynamoRetryClassifier")
    private Object[][] provideDynamoRetryClassifierTestData() {
        return new Object[][] { //
                { new ProvisionedThroughputExceededException(""), true }, //
                { new LimitExceededException(""), true }, //
                { new ItemCollectionSizeLimitExceededException(""), true }, //
                { new TransactionConflictException(""), true }, //
                { new InternalServerErrorException(""), true }, //
                { new ResourceInUseException(""), false }, //
                { new ResourceNotFoundException(""), false }, //
                { new ConditionalCheckFailedException(""), false }, //
                { new TransactionCanceledException(""), false }, //
                /*-
                 * base exceptions with different error codes
                 */
                { newExceptionWithCode("ThrottlingException"), true }, //
                { newExceptionWithCode("ThrottlingError"), true }, //
                { newExceptionWithCode("ThrottlingErrorException"), true }, //
                { newExceptionWithCode("ProvisionedThroughputExceeded"), true }, //
                { newExceptionWithCode("ProvisionedThroughputExceededException"), true }, //
                { newExceptionWithCode("Some other error msg"), false }, //
        }; //
    }

    @DataProvider(name = "dynamoThrottlingException")
    private Object[][] provideDynamoThrottlingExceptionTestData() {
        return new Object[][] { //
                { new ProvisionedThroughputExceededException(""), true }, //
                { new LimitExceededException(""), false }, //
                { new ItemCollectionSizeLimitExceededException(""), false }, //
                { new TransactionConflictException(""), false }, //
                { new InternalServerErrorException(""), false }, //
                { new ResourceInUseException(""), false }, //
                { new ResourceNotFoundException(""), false }, //
                { new ConditionalCheckFailedException(""), false }, //
                { new TransactionCanceledException(""), false }, //
                /*-
                 * base exceptions with different error codes
                 */
                { newExceptionWithCode("ThrottlingException"), true }, //
                { newExceptionWithCode("ThrottlingError"), true }, //
                { newExceptionWithCode("ThrottlingErrorException"), true }, //
                { newExceptionWithCode("ProvisionedThroughputExceeded"), true }, //
                { newExceptionWithCode("ProvisionedThroughputExceededException"), true }, //
                { newExceptionWithCode("Some other error msg"), false }, //
        }; //
    }

    private AmazonDynamoDBException newExceptionWithCode(@NotNull String code) {
        AmazonDynamoDBException e = new AmazonDynamoDBException("");
        e.setErrorCode(code);
        return e;
    }

}
