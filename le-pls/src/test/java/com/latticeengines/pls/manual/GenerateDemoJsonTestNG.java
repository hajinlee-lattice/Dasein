package com.latticeengines.pls.manual;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.TopNAttributes;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;

public class GenerateDemoJsonTestNG {

    private Random random = new Random();

    @Test(groups = "manual")
    public void generateJson() throws IOException {
        String path = ClassLoader.getSystemResource("com/latticeengines/pls/manual/category-taxonomy.csv").getPath();
        File file = new File(path);
        CSVParser parser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.RFC4180);
        List<LeadEnrichmentAttribute> attributes = new ArrayList<>();
        AccountMasterCube cube = new AccountMasterCube();
        cube.setStatistics(new HashMap<>());
        Map<String, TopNAttributes> topn = new HashMap<>();
        for (CSVRecord record : parser) {
            String category = record.get(0);
            String subcategory = record.get(1);
            if (StringUtils.isEmpty(subcategory)) {
                subcategory = "Other";
            }
            String attribute = record.get(2);
            String value = record.get(3);
            String description = record.get(4);
            String fieldName = "Demo - " + subcategory + " - " + attribute;
            if (!attributes.stream().anyMatch(a -> a.getFieldName().equals(fieldName))) {
                attributes.add(createLeadEnrichmentAttribute(category, subcategory, attribute, fieldName, description));
            }
            AttributeStatistics statistics = cube.getStatistics().get(fieldName);
            if (statistics == null) {
                statistics = new AttributeStatistics();
                cube.getStatistics().put(fieldName, statistics);
            }
            addBucket(statistics, value);

            TopNAttributes topNAttributes = topn.get(category);
            if (topNAttributes == null) {
                topNAttributes = new TopNAttributes();
                topNAttributes.setTopAttributes(new HashMap<>());
                topn.put(category, topNAttributes);
            }
            List<TopNAttributes.TopAttribute> attributesInSubcategory = topNAttributes.getTopAttributes().get(
                    subcategory);
            if (attributesInSubcategory == null) {
                attributesInSubcategory = new ArrayList<>();
                topNAttributes.getTopAttributes().put(subcategory, attributesInSubcategory);
            }

            if (!attributesInSubcategory.stream().anyMatch(a -> a.getAttribute().equals(fieldName))) {
                attributesInSubcategory.add(createTopAttribute(fieldName));
            }
        }
        FileUtils.write(new File("/tmp/buckets.json"), JsonUtils.serialize(cube));
        FileUtils.write(new File("/tmp/attributes.json"), JsonUtils.serialize(attributes));
        FileUtils.write(new File("/tmp/topn.json"), JsonUtils.serialize(topn));
    }

    private TopNAttributes.TopAttribute createTopAttribute(String fieldName) {
        TopNAttributes.TopAttribute topAttribute = new TopNAttributes.TopAttribute(fieldName,
                (long) random.nextInt(15000));
        return topAttribute;
    }

    private void addBucket(AttributeStatistics statistics, String value) {
        if (statistics.getRowBasedStatistics() == null) {
            statistics.setRowBasedStatistics(new AttributeStatsDetails());
            statistics.getRowBasedStatistics().setNonNullCount((long) random.nextInt(15000));
        }
        if (statistics.getRowBasedStatistics().getBuckets() == null) {
            statistics.getRowBasedStatistics().setBuckets(new Buckets());
            statistics.getRowBasedStatistics().setNonNullCount(0L);
        }
        Buckets buckets = statistics.getRowBasedStatistics().getBuckets();
        if (buckets.getBucketList() == null) {
            buckets.setBucketList(new ArrayList<>());
        }
        buckets.setType(BucketType.Boolean);
        Bucket bucket = new Bucket();
        bucket.setBucketLabel(value);
        bucket.setCount((long) random.nextInt(15000));
        buckets.getBucketList().add(bucket);
        statistics.getRowBasedStatistics().setNonNullCount(
                statistics.getRowBasedStatistics().getNonNullCount() + bucket.getCount());
    }

    private LeadEnrichmentAttribute createLeadEnrichmentAttribute(String category, String subcategory,
            String displayName, String fieldName, String description) {
        LeadEnrichmentAttribute attribute = new LeadEnrichmentAttribute();
        attribute.setDisplayName(displayName);
        attribute.setFieldName(fieldName);
        attribute.setCategory(category);
        attribute.setSubcategory(subcategory);
        attribute.setDescription(description);
        attribute.setFundamentalType(FundamentalType.BOOLEAN);
        return attribute;
    }
}
