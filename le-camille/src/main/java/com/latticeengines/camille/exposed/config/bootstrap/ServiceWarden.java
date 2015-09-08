package com.latticeengines.camille.exposed.config.bootstrap;

import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.messaging.MessageConsumer;
import com.latticeengines.camille.exposed.messaging.MessageQueue;
import com.latticeengines.camille.exposed.messaging.MessageQueueFactory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.ServiceInfo;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

/**
 * A high-level class responsible for service registration and bootstrap.
 */
public class ServiceWarden {
    /**
     * To be invoked from code residing within the service itself.
     */
    public static void registerService(String serviceName, ServiceInfo info) {
        log.info("Registering service {} with properties {}", serviceName, info.properties);
        CustomerSpaceServiceBootstrapManager
                .register(serviceName, info.properties, info.cssInstaller, info.cssUpgrader);
        ServiceBootstrapManager.register(serviceName, info.properties, info.installer);

        BootstrapMessageConsumer consumer = new BootstrapMessageConsumer();
        MessageQueueFactory factory = MessageQueueFactory.instance();
        factory.construct(BootstrapMessage.class, getBootstrapMessageQueueName(serviceName), consumer);
    }

    private static String getBootstrapMessageQueueName(String serviceName) {
        if (serviceName == null || serviceName == "") {
            throw new IllegalArgumentException("In getBootstrapMessageQueueName, serviceName cannot be null or empty");
        }

        return "bootstrap_" + serviceName;
    }

    /**
     * This will send a message to invoke service and customerspaceservice
     * bootstrap.
     */
    public static void commandBootstrap(String serviceName, CustomerSpace space, Map<String, String> bootstrapProperties) {
        log.info("Sending bootstrap message to service {} and space {}", serviceName, space);
        MessageQueueFactory factory = MessageQueueFactory.instance();
        MessageQueue<BootstrapMessage> queue = factory.construct(BootstrapMessage.class,
                getBootstrapMessageQueueName(serviceName), null);
        queue.put(new BootstrapMessage(serviceName, space, bootstrapProperties));
    }

    public static class BootstrapMessageConsumer extends MessageConsumer<BootstrapMessage> {

        @Override
        public void consume(BootstrapMessage message) {
            try {
                ServiceScope serviceScope = new ServiceScope(message.serviceName, message.bootstrapProperties);
                CustomerSpaceServiceScope cssScope = new CustomerSpaceServiceScope(message.space, message.serviceName,
                        message.bootstrapProperties);
                ServiceBootstrapManager.bootstrap(serviceScope);
                CustomerSpaceServiceBootstrapManager.bootstrap(cssScope);
            } catch (Exception e) {
                String error = String
                        .format("Unexpected error encountered attempting to consume bootstrap message for service %s and space %s: %s",
                                message.serviceName, message.space, e.getMessage());
                log.error(error, e);
                throw new RuntimeException(error, e);
            }
        }
    }

    public static class BootstrapMessage {
        public BootstrapMessage(String serviceName, CustomerSpace space, Map<String, String> bootstrapProperties) {
            this.serviceName = serviceName;
            this.space = space;
            this.bootstrapProperties = bootstrapProperties;
        }

        // Serialization constructor
        public BootstrapMessage() {
        }

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this);
        }

        public String serviceName;
        public CustomerSpace space;
        public Map<String, String> bootstrapProperties;
    }

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());
}
