package com.latticeengines.lambda.authorizer.config;

public abstract class AuthorizerConfigBase implements AuthorizerConfig {

    @Override
    public String getUlyssesBase() {
        return getApiHost() + "/ulysses";
    }

    @Override
    public RetryPolicy getRetryPolicy() {
        return new RetryPolicy() {
            @Override
            public int getMaxAttemps() {
                return 3;
            }
        };
    }

    @Override
    public BackOffPolicy getBackOffPolicy() {
        return new BackOffPolicy() {
            @Override
            public int getInitialInterval() {
                return 500;
            }

            @Override
            public int getMultiplier() {
                return 2;
            }
        };
    }
    
}
