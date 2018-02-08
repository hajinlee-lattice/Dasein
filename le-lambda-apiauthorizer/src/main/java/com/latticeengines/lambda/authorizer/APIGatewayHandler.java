package com.latticeengines.lambda.authorizer;

import com.amazonaws.services.lambda.runtime.Context;
import com.latticeengines.lambda.authorizer.domain.AuthPolicy;
import com.latticeengines.lambda.authorizer.domain.TokenAuthorizerContext;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.AbstractEnvironment;

/**
 * This is the main entry point for your Lambda function. If you were to run
 * this on AWS, your handler path would be the fully qualified class name,
 * com.latticeengines.lambda.authorizer.MainHandler.
 * <p>
 * Note that this class doesn't contain any logic itself and exists solely to
 * provide the application context. That's because the method
 * {@link SpringRequestHandler#handleRequest(Object, Context)} actually uses the
 * provided application context to find a bean of type
 * {@link com.amazonaws.services.lambda.runtime.RequestHandler}, and calls it's
 * "handleRequest" method given the input object.
 * <p>
 * Of course, you are free to override the "handleRequest" method if you want
 * to, however this default pattern should work for most use cases.
 * <p>
 * The generic type parameters for this class represent the input and output
 * types of your {@link com.amazonaws.services.lambda.runtime.RequestHandler}
 * bean. These MUST match otherwise you will run into a runtime error.
 * <p>
 */
public class APIGatewayHandler extends SpringRequestHandler {

    /**
     * AWS lambda does not allow System variable name with '.'. So, could not use standard "spring.profiles.active".
     */
    private static final String ACTIVE_PROFILE = "activeProfile";
    
    /**
     * Here we create the Spring {@link ApplicationContext} that will be used
     * throughout our application.
     */
    private static ApplicationContext applicationContext = null;
    
    static {
        if (System.getProperty(AbstractEnvironment.ACTIVE_PROFILES_PROPERTY_NAME) == null) {
            System.setProperty(AbstractEnvironment.ACTIVE_PROFILES_PROPERTY_NAME, System.getenv(ACTIVE_PROFILE));
        }
        applicationContext = new AnnotationConfigApplicationContext(ApplicationConfiguration.class);
    }
    
    @Override
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }
}
