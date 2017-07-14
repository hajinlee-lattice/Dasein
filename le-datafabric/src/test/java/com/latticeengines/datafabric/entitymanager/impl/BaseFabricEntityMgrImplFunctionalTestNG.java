package com.latticeengines.datafabric.entitymanager.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.datafabric.entitymanager.FabricEntityProcessor;
import com.latticeengines.datafabric.functionalframework.DataFabricFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datafabric.TopicScope;

public class BaseFabricEntityMgrImplFunctionalTestNG extends DataFabricFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(BaseFabricEntityMgrImplFunctionalTestNG.class);

    private final String latticeId = "12345654321";

    private SampleFabricEntityMgr entityManager;
    private SampleEntityProcessor processor;

    private String store;
    private String repository;
    private String recordType;
    private String topic;
    private String topicToConnect;
    private String group;

    private boolean connectorEnabled = true;

    int messageCount = 0;
    int invalidMessages = 0;

    @BeforeMethod(groups = "functional")
    public void setUp() throws Exception {
        topic = "demoEntityTopic";
        topicToConnect = "demoEntityTopicToConnect";
        store = "REDIS";
        repository = "demoRepository";
        recordType = "demoRecord";
        group = "demoGroup";

        messageService.createTopic(topic, TopicScope.PRIVATE, 1, 1);
        messageService.createTopic(topicToConnect, TopicScope.PRIVATE, 1, 1);

        SampleFabricEntityMgr.Builder builder = new SampleFabricEntityMgr.Builder().messageService(messageService)
                .dataService(dataService).topic(topic).store(store).repository(repository).recordType(recordType);

        entityManager = new SampleFabricEntityMgr(builder);
        entityManager.init();

        processor = new SampleEntityProcessor();
        entityManager.addConsumer(group, processor, 1);

        List<SampleEntity> entities = createEntities(16);
        deleteEntities(entities);
    }

    @AfterMethod(groups = "functional")
    public void tearDown() throws Exception {

        entityManager.removeConsumer(group, 12000);
        // if (messageService.topicExists(topic)) {
        // messageService.deleteTopic(topic, false);
        // }
        // if (messageService.topicExists(topicToConnect)) {
        // messageService.deleteTopic(topicToConnect, false);
        // }
    }

    @Test(groups = "functional", enabled = false)
    public void testCreateAndFindAndDelete() throws Exception {

        List<SampleEntity> entities = createEntities(16);

        for (SampleEntity entity : entities) {
            entityManager.create(entity);
        }

        List<SampleEntity> results = new ArrayList<SampleEntity>();
        ;
        for (SampleEntity entity : entities) {
            results.add(entityManager.findByKey(entity));
        }

        compareEntities(entities, results);

        results = entityManager.findByLatticeId(latticeId);

        validateEntities(results, 16);

        deleteEntities(entities);
    }

    @Test(groups = "functional", enabled = true)
    public void testPublishAndConsume() throws Exception {

        List<SampleEntity> entities = createEntities(16);

        processor.entityCount = 0;
        processor.invalidEntities = 0;

        // if (true) return;

        for (SampleEntity entity : entities) {
            entityManager.publish(entity);
        }

        try {
            Thread.sleep(4000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        Assert.assertEquals(processor.entityCount, 16);
        Assert.assertEquals(processor.invalidEntities, 0);

        deleteEntities(entities);
    }

    @Test(groups = "functional", enabled = false)
    public void testConnector() throws Exception {

        if (!connectorEnabled)
            return;

        List<SampleEntity> entities = createEntities(16);

        for (SampleEntity entity : entities) {
            entityManager.publish(entity);
        }

        try {
            Thread.sleep(4000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        List<SampleEntity> results = new ArrayList<SampleEntity>();
        ;
        for (SampleEntity entity : entities) {
            results.add(entityManager.findByKey(entity));
        }

        compareEntities(entities, results);

        results = entityManager.findByLatticeId(latticeId);

        validateEntities(results, 16);

        deleteEntities(entities);

    }

    public class SampleEntityProcessor implements FabricEntityProcessor {

        private int entityCount = 0;
        private int invalidEntities = 0;

        public void process(Object obj) {
            log.info("Process entity " + entityCount + "\n");
            entityCount++;
            SampleEntity entity = (SampleEntity) obj;
            if (!entity.getCompany().equals("myCompany"))
                invalidEntities++;
            else if (!entity.getLatticeId().equals(latticeId))
                invalidEntities++;
        }
    }

    private List<SampleEntity> createEntities(int numEntities) {
        List<SampleEntity> entities = new ArrayList<SampleEntity>();
        for (int i = 0; i < numEntities; i++) {
            SampleEntity entity = new SampleEntity();
            entity.setId(i + "");
            entity.setCompany("myCompany");
            entity.setLatticeId(latticeId);
            entity.setName("name" + i);
            entities.add(entity);
        }
        return entities;
    }

    private void deleteEntities(List<SampleEntity> entities) {

        for (SampleEntity entity : entities) {
            entityManager.delete(entity);
        }

        for (SampleEntity entity : entities) {
            Assert.assertEquals(entityManager.findByKey(entity), null);
        }
        List<SampleEntity> results = entityManager.findByLatticeId(latticeId);
        if (results != null) {
            Assert.assertEquals(results.size(), 0);
        }
    }

    private void compareEntities(List<SampleEntity> sources, List<SampleEntity> dests) {
        Assert.assertEquals(sources.size(), dests.size());
        Iterator<SampleEntity> srcItr = sources.iterator();
        Iterator<SampleEntity> destItr = dests.iterator();
        while (srcItr.hasNext()) {
            SampleEntity src = srcItr.next();
            SampleEntity dest = destItr.next();
            Assert.assertEquals(src.getId(), dest.getId());
            Assert.assertEquals(src.getName(), dest.getName());
            Assert.assertEquals(src.getLatticeId(), dest.getLatticeId());
        }
    }

    private void validateEntities(List<SampleEntity> entities, int number) {

        Assert.assertEquals(entities.size(), number);
        Iterator<SampleEntity> srcItr = entities.iterator();
        while (srcItr.hasNext()) {
            SampleEntity src = srcItr.next();
            Assert.assertTrue(src.getName().startsWith("name"));
            Assert.assertEquals(src.getLatticeId(), latticeId);
            Assert.assertEquals(src.getCompany(), "myCompany");
        }
    }
}
