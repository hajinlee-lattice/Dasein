package com.latticeengines.camille.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.messaging.MessageConsumer;
import com.latticeengines.camille.exposed.messaging.MessageQueue;
import com.latticeengines.camille.exposed.messaging.MessageQueueFactory;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;

public class MessageQueueUnitTestNG {
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
        public final static String MESSAGE_TEXT = "Hello World!";

        public Message() {
            text = MESSAGE_TEXT;
        }

        public String getText() {
            return text;
        }

        private String text;
    }

    public static class Consumer extends MessageConsumer<Message> {

        @Override
        public void consume(Message message) {
            Assert.assertEquals(message.getText(), Message.MESSAGE_TEXT);
            received = true;
        }

        public boolean received;
    }

    @Test(groups = "unit", timeOut = 60000)
    public void testSendMessage() throws InterruptedException {
        MessageQueueFactory factory = MessageQueueFactory.instance();
        factory.clearCache();
        Consumer consumer = new Consumer();
        MessageQueue<Message> queue = factory.construct(Message.class, "myqueue", consumer);
        queue.put(new Message());
        while (true) {
            if (consumer.received) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    @Test(groups = "unit", expectedExceptions = Exception.class)
    public void testInvalidQueueName1() {
        MessageQueueFactory factory = MessageQueueFactory.instance();
        factory.construct(Message.class, "", new Consumer());
    }

    @Test(groups = "unit", expectedExceptions = Exception.class)
    public void testInvalidQueueName2() {
        MessageQueueFactory factory = MessageQueueFactory.instance();
        factory.construct(Message.class, null, new Consumer());
    }

    @Test(groups = "unit", expectedExceptions = Exception.class)
    public void testDuplicateQueueName() {
        MessageQueueFactory factory = MessageQueueFactory.instance();
        factory.clearCache();
        Consumer consumer = new Consumer();
        @SuppressWarnings("unused")
        MessageQueue<Message> queue = factory.construct(Message.class, "myqueue", consumer);
        @SuppressWarnings("unused")
        MessageQueue<Object> queue2 = factory.construct(Object.class, "myqueue", new MessageConsumer<Object>() {

            @Override
            public void consume(Object message) {
                // pass
            }
        });

    }

    @Test(groups = "unit")
    public void testSendOnly() throws InterruptedException {
        MessageQueueFactory factory = MessageQueueFactory.instance();
        MessageQueue<Message> queue = factory.construct(Message.class, "sendonly", null);
        queue.put(new Message());
        Thread.sleep(1000);
        // Not sure how we'd test that no consumer was triggered internally.
    }

    @Test(groups = "unit", timeOut = 60000)
    public void testDivAndShare() throws InterruptedException {
        CamilleTestEnvironment.setDivision("a", "bootstrap_Dante, bootstrap_Me");
        MessageQueueFactory factory = MessageQueueFactory.instance();
        factory.clearCache();
        Consumer consumerA = new Consumer();
        MessageQueue<Message> queue = factory.construct(Message.class, "myqueue", consumerA);
        queue.put(new Message());
        while (true) {
            if (consumerA.received) {
                break;
            }
            Thread.sleep(1000);
        }

        Consumer consumerB = new Consumer();
        queue = factory.construct(Message.class, "bootstrap_Dante", consumerB);
        queue.put(new Message());
        while (true) {
            if (consumerB.received) {
                break;
            }
            Thread.sleep(1000);
        }
    }
}
