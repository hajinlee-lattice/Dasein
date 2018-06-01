package com.latticeengines.datacloud.collection.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@DirtiesContext
@ContextConfiguration(locations = {"classpath:test-datacloud-collection-context.xml"})
public class CollectionDBServiceTestNG extends AbstractTestNGSpringContextTests {
    @Inject
    CollectionDBService collectionDBService;

    @Value("${datacloud.collection.test.domains}")
    String testDomains;

    @Test(groups = "functional")
    public void testCollectionDBService() throws Exception {
        List<String> domains = new ArrayList<String>(Arrays.asList((testDomains).split(",")));
        collectionDBService.addNewDomains(domains, "builtwith", UUID.randomUUID().toString().toUpperCase());

        /*while (true)
        {
            collectionDBService.service();

            Thread.sleep(15000);
        }*/
    }
}
