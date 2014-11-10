package com.latticeengines.scheduler.fairscheduler.generator;

import java.io.File;
import java.math.BigInteger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

public class AllocationsGenerator {

    private static final String MAX_RUNNING_APPS = "33";
    private static final int NUM_MR_LEAF_QUEUES = 12;
    private static final int NUM_NON_MR_LEAF_QUEUES = 33;
    private static final String FIFO = "fifo";
    private static final String MR_MIN_RESOURCES = "43008";
    private static final String NON_MR_MIN_RESOURCES = "15360";

    public static void main(String[] args) throws Exception {

        ObjectFactory factory = new ObjectFactory();

        Allocations allocations = factory.createAllocations();

        Queue p0MRQ = factory.createQueue();
        p0MRQ.setName("Priority0.MapReduce");
        p0MRQ.setWeight(new BigInteger("1000"));
        for (int i = 0; i < NUM_MR_LEAF_QUEUES; i++) {
            Queue leaf = factory.createQueue();
            leaf.setName(Integer.toString(i));
            leaf.setMinResources(MR_MIN_RESOURCES + " mb, 12 vcores");
            leaf.setSchedulingPolicy(FIFO);
            p0MRQ.getQueue().add(leaf);
        }
        allocations.getQueue().add(p0MRQ);

        Queue p0 = factory.createQueue();
        p0.setName("Priority0");
        p0.setWeight(new BigInteger("100"));
        addLeaves(factory, p0, true, NUM_NON_MR_LEAF_QUEUES);
        allocations.getQueue().add(p0);

        Queue p1 = factory.createQueue();
        p1.setName("Priority1");
        p1.setWeight(new BigInteger("10"));
        addLeaves(factory, p1, false, NUM_NON_MR_LEAF_QUEUES);
        allocations.getQueue().add(p1);

        Queue p2 = factory.createQueue();
        p2.setName("Priority2");
        p2.setWeight(new BigInteger("1"));
        addLeaves(factory, p2, false, NUM_NON_MR_LEAF_QUEUES);
        allocations.getQueue().add(p2);

        allocations.setDefaultMinSharePreemptionTimeout(new BigInteger("30"));
        allocations.setFairSharePreemptionTimeout(new BigInteger("30"));

        User user = factory.createUser();
        user.setName("yarn");
        user.setMaxRunningApps(new BigInteger(MAX_RUNNING_APPS));

        allocations.setUser(user);

        Marshaller marshaller = JAXBContext.newInstance("com.latticeengines.scheduler.fairscheduler.generator").createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(allocations, new File("./fair-scheduler.xml"));
    }

    static void addLeaves(ObjectFactory factory, Queue q, boolean minResources, int numLeaves) {
        for (int i = 0; i < numLeaves; i++) {
            Queue leaf = factory.createQueue();
            leaf.setName(Integer.toString(i));
            if (minResources) {
                leaf.setMinResources(NON_MR_MIN_RESOURCES + " mb, 4 vcores");
            }
            leaf.setSchedulingPolicy(FIFO);
            q.getQueue().add(leaf);
        }
    }
}
