package com.latticeengines.lambda.authorizer.config;

public interface AuthorizerConfig {

    public String getApiHost();
    
    public String getUlyssesBase();

    public RetryPolicy getRetryPolicy();
    
    public BackOffPolicy getBackOffPolicy();
    
    public interface RetryPolicy {
        int getMaxAttemps();
    }
    
    public interface BackOffPolicy {
        int getInitialInterval();
        int getMultiplier();
    }
}
