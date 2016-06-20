package com.latticeengines.datafabric.service.message.impl;

import com.latticeengines.datafabric.service.message.FabricMessageConsumer;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;


public class SimpleFabricMessageConsumerImpl implements FabricMessageConsumer {

    private static final Log log = LogFactory.getLog(SimpleFabricMessageConsumerImpl.class);

    private String groupId;

    private String topic;

    private boolean autoCommit;

    private int numThreads;

    private boolean autoStart;

    private String streamProc;

    private boolean shared;

    @Autowired
    private FabricMessageService messageService;

    private ConsumerConnector consumer;
    private ExecutorService executor;
    FabricStreamProc processor;
    private Properties props;
    private String derivedTopic;

    public SimpleFabricMessageConsumerImpl(Builder builder) {
        this.groupId = builder.group;
        this.topic = builder.topic;
        this.shared = builder.shared;
        this.numThreads = builder.numThreads;
        this.autoStart = builder.autoStart;
        this.autoCommit = builder.autoCommit;
        this.processor = builder.processor;
        if (builder.messageService != null) this.messageService = builder.messageService;

        this.derivedTopic = messageService.deriveTopic(topic, shared);
        this.props = createConsumerConfig();
        if (this.autoStart == true) {
            start();
        }
    }

    public void start() {


        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        VerifiableProperties vProps = new VerifiableProperties(props);

        // Launch all the threads

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        log.info("Listening to " + derivedTopic);
        topicCountMap.put(derivedTopic, numThreads);

        // Create decoders for key and value
        KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(vProps);

        Map<String, List<KafkaStream<Object, Object>>> consumerMap =
            consumer.createMessageStreams(topicCountMap, avroDecoder, avroDecoder);
        List<KafkaStream<Object, Object>> streams = consumerMap.get(derivedTopic);

        executor = Executors.newFixedThreadPool(numThreads);

        try {
//            Constructor<?> constructor = streamProcClass.getConstructor(KafkaStream);
            // Create stream processors and bind them to threads
            int threadNumber = 0;
            for (final KafkaStream stream : streams) {
                FabricStreamRunnable runnable = new FabricStreamRunnable(stream, processor);

                executor.submit(runnable);
                threadNumber++;
            }
        } catch (Exception ex) {
            log.error("Failed to start stream consumers");
            log.error(ex);
        }
    }

    public void stop(int waitTime) {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(waitTime, TimeUnit.MILLISECONDS)) {
                log.info( "Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            log.info("Interrupted during shutdown, exiting uncleanly");
        }
    }


    private Properties createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", messageService.getZkConnect());
        props.put("group.id", this.groupId);
        props.put("schema.registry.url", messageService.getSchemaRegUrl());
        props.put("specific.avro.reader", false);

        // We configure the consumer to avoid committing offsets and to always start consuming from beginning of topic
        // This is not a best practice, but we want the example consumer to show results when running it again and again
//        props.put("auto.commit.enable", autoCommit);
        props.put("auto.offset.reset", "smallest");

        return props;
    }

    private class FabricStreamRunnable implements Runnable {


        private KafkaStream stream;
        private FabricStreamProc processor;

        public FabricStreamRunnable(KafkaStream stream, FabricStreamProc processor) {
            this.stream = stream;
            this.processor = processor;
        }

        public void run() {
            ConsumerIterator it = stream.iterator();

            while (it.hasNext()) {
                MessageAndMetadata record = it.next();
                GenericRecord key = (GenericRecord)record.key();
                GenericRecord value = (GenericRecord)record.message();
                if (key.get("record") == null) {
                    continue;
                }
                if (key.get("id") == null) {
                    continue;
                }
                processor.processRecord(key.get("record").toString(), key.get("id").toString(), value);
            }
        }
    }


    public static class Builder {

        private String group;

        private String topic;

        private String streamProc;

        private FabricStreamProc processor;

        private boolean autoCommit = true;

        private int numThreads = 1;

        private boolean autoStart = true;

        private boolean shared = false;;

        private FabricMessageService messageService = null;

        public Builder() {
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder processor(FabricStreamProc processor) {
            this.processor = processor;
            return this;
        }

        public Builder autoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder autoStart(boolean autoStart) {
            this.autoStart = autoStart;
            return this;
        }

        public Builder numThreads(int numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        public Builder shared(boolean shared) {
            this.shared = shared;
            return this;
        }

        public Builder messageService(FabricMessageService messageService) {
            this.messageService = messageService;
            return this;
        }
    }
}
