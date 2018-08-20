package com.latticeengines.datacloud.collection.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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

    List<String> loadDomains(String path) throws Exception
    {
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',')
                .withIgnoreEmptyLines(true).withIgnoreSurroundingSpaces(true);

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        List<String> ret = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(classloader.getResourceAsStream(path)))) {
            try (CSVParser parser = new CSVParser(reader, format)) {
                Map<String, Integer> colMap = parser.getHeaderMap();
                int domainIdx = colMap.getOrDefault("Domain", -1);

                Iterator<CSVRecord> ite = parser.iterator();
                while (ite.hasNext()) {
                    CSVRecord rec = ite.next();
                    ret.add(rec.get(domainIdx));
                }
            }
        }

        return ret;
    }

    @Test(groups = "load_test")
    public void testLoad() throws Exception {
        List<String> path_set = new ArrayList<>();
        path_set.add("50kdomains.0.csv");
        path_set.add("50kdomains.1.csv");
        path_set.add("50kdomains.2.csv");
        path_set.add("50kdomains.3.csv");

        List<List<String>> domains_set = new ArrayList<>();
        for (int i = 0; i < path_set.size(); ++i)
        {
            domains_set.add(loadDomains(path_set.get(i)));
        }

        long lastTriggered = 0;
        int domains_idx = 0;
        while (true)
        {
            long curMillis = System.currentTimeMillis();
            if (lastTriggered == 0 || curMillis - lastTriggered > 10 * 60 * 1000)
            {
                lastTriggered = curMillis;
                collectionDBService.addNewDomains(domains_set.get(domains_idx), "builtwith", UUID.randomUUID()
                        .toString().toUpperCase());

                domains_idx = (domains_idx + 1) % domains_set.size();
            }

            collectionDBService.service();
            Thread.sleep(15 * 1000);
        }
    }

    @Test(groups = "normal_test")
    public void testCollectionDBService() throws Exception {
        List<String> domains = new ArrayList<String>(Arrays.asList(testDomains.split(",")));
        collectionDBService.addNewDomains(domains, "builtwith", UUID.randomUUID().toString().toUpperCase());


        while (true)
        {
            collectionDBService.service();

            Thread.sleep(15000);
        }
    }
}
