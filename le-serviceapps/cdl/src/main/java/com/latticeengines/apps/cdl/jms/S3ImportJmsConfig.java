package com.latticeengines.apps.cdl.jms;

import javax.inject.Inject;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.destination.DynamicDestinationResolver;
import org.springframework.util.ErrorHandler;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;

@Configuration
@EnableJms
public class S3ImportJmsConfig {

    private static final Logger log = LoggerFactory.getLogger(S3ImportJmsConfig.class);


    @Inject
    private SQSConnectionFactory sqsConnectionFactory;

    @Value("${cdl.s3.sqs.listener.start}")
    private Boolean autoStart;

    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory() {
        DefaultJmsListenerContainerFactory factory
                = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(this.sqsConnectionFactory);
        factory.setDestinationResolver(new DynamicDestinationResolver());
        factory.setConcurrency("3-10");
        factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        factory.setErrorHandler(messageError());
        factory.setAutoStartup(Boolean.TRUE.equals(this.autoStart));
        return factory;
    }

    @Bean
    public JmsTemplate jmsTemplate() {
        return new JmsTemplate(this.sqsConnectionFactory);
    }

    @Bean
    public ErrorHandler messageError() {
        return throwable -> log.error(throwable.getMessage());
    }

}
