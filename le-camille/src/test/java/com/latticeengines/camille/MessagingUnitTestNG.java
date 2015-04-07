package com.latticeengines.camille;

import java.util.AbstractMap;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class MessagingUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    public static class Message {
        public Message(String message) {
            this.message = message;
        }

        // Serialization Constructor.
        public Message() {
        }

        public String message;
    }

    public static class Serializer implements QueueSerializer<Message> {

        @Override
        public byte[] serialize(Message item) {
            return JsonUtils.serialize(item).getBytes();
        }

        @Override
        public Message deserialize(byte[] bytes) {
            String json = new String(bytes);
            return JsonUtils.deserialize(json, Message.class);
        }

    }

    public static class Consumer implements QueueConsumer<Message> {

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            // TODO Auto-generated method stub

        }

        @Override
        public void consumeMessage(Message message) throws Exception {
            System.out.println(message.message);
        }
    }

    @Test(groups = "unit")
    public void testSReceive() throws Exception {
        Serializer serializer = new Serializer();
        Consumer consumer = new Consumer();

        QueueBuilder<Message> builder = QueueBuilder.builder(CamilleEnvironment.getCamille().getCuratorClient(),
                consumer, serializer, "/Pods/Foo" + CamilleEnvironment.getPodId());
        DistributedQueue<Message> queue = builder.buildQueue();

        queue.start();

        Thread.sleep(10000000);
        queue.close();

        List<AbstractMap.SimpleEntry<Document, Path>> children = CamilleEnvironment.getCamille().getChildren(
                new Path("/Pods/Foo" + CamilleEnvironment.getPodId()));
    }
}
