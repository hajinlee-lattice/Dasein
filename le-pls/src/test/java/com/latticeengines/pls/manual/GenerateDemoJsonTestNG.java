package com.latticeengines.pls.manual;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;

public class GenerateDemoJsonTestNG {

    private Random random = new Random();

    @Test(groups = "manual")
    public void generateJson() throws IOException {
        File file = new File("/home/bross/Downloads/category-taxonomy.csv");
        CSVParser parser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.RFC4180);
        List<LeadEnrichmentAttribute> attributes = new ArrayList<>();
        AccountMasterCube cube = new AccountMasterCube();
        cube.setStatistics(new HashMap<>());
        for (CSVRecord record : parser) {
            String category = record.get(0);
            String subcategory = record.get(1);
            String attribute = record.get(2);
            String value = record.get(3);

            attributes.add(createLeadEnrichmentAttribute(category, subcategory, attribute));
            AttributeStatistics statistics = cube.getStatistics().get(attribute);
            if (statistics == null) {
                statistics = new AttributeStatistics();
                cube.getStatistics().put(attribute, statistics);
            }
            addBucket(statistics, value);
        }
        FileUtils.write(new File("/tmp/buckets.json"), JsonUtils.serialize(cube));
        FileUtils.write(new File("/tmp/attributes.json"), JsonUtils.serialize(attributes));
    }

    private void addBucket(AttributeStatistics statistics, String value) {
        if (statistics.getRowBasedStatistics() == null) {
            statistics.setRowBasedStatistics(new AttributeStatsDetails());
        }
        if (statistics.getRowBasedStatistics().getBuckets() == null) {
            statistics.getRowBasedStatistics().setBuckets(new Buckets());
        }
        Buckets buckets = statistics.getRowBasedStatistics().getBuckets();
        if (buckets.getBucketList() == null) {
            buckets.setBucketList(new ArrayList<>());
        }
        Bucket bucket = new Bucket();
        bucket.setBucketLabel(value);
        bucket.setCount((long) random.nextInt(15000));
        buckets.getBucketList().add(bucket);
    }

    private LeadEnrichmentAttribute createLeadEnrichmentAttribute(String category, String subcategory, String name) {
        LeadEnrichmentAttribute attribute = new LeadEnrichmentAttribute();
        attribute.setFieldName(name);
        attribute.setCategory(category);
        attribute.setSubcategory(subcategory);
        return attribute;
    }
}
