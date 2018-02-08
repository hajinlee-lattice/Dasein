package com.latticeengines.lambda.authorizer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.latticeengines.lambda.authorizer.domain.AuthPolicy;
import com.latticeengines.lambda.authorizer.domain.TokenAuthorizerContext;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;

/**
 * Central class to extend to provide Spring support for your AWS Lambda
 * applications. The key to using this class is to implement the
 * {@link SpringRequestHandler#getApplicationContext()} method and supply a
 * valid {@link ApplicationContext} instance. Your child class that extends
 * this class will be the entry point (handler) to your AWS Lambda function.
 * <p>
 * The {@link SpringRequestHandler#handleRequest(Object, Context)} method is
 * left virtual so it can be overridden, but the default implementation should
 * work for most use cases. It simply grabs a bean of type
 * {@link RequestHandler} and calls that class's
 * {@link RequestHandler#handleRequest(Object, Context)} method. In this sense,
 * this class is just a wrapper around your implementation of
 * {@link RequestHandler}, but allows your implementation to take advantage of
 * Spring's core features (dependency injection, etc).
 *
 * @param <I> The input type parameter. NOTE: this type must match the input
 *            type parameter of your {@link RequestHandler} bean.
 * @param <O> The output type parameter. NOTE: this type must match the input
 *            type parameter of your {@link RequestHandler} bean.
 */
public abstract class SpringRequestHandler implements RequestHandler<TokenAuthorizerContext, AuthPolicy>, ApplicationContextProvider {

    private static Logger LOG = Logger.getLogger(SpringRequestHandler.class);
    
    private final RequestHandler<TokenAuthorizerContext, AuthPolicy> handler;

    public SpringRequestHandler() {
        handler = getApplicationContext().getBean(RequestHandler.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public AuthPolicy handleRequest(final TokenAuthorizerContext input, final Context context) {
        return (AuthPolicy) handler.handleRequest(input, context);
    }
}
