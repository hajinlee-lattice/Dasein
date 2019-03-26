package com.latticeengines.datacloud.etl.transformation.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.domain.exposed.datacloud.dataflow.CategoricalBucket;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CalculateStatsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ProfileConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class ContactStatisticsTestNG extends AccountMasterBucketTestNG {

    private static final Logger log = LoggerFactory.getLogger(ContactStatisticsTestNG.class);

    private GeneralSource contact = new GeneralSource("Contact");
    private GeneralSource contactProfile = new GeneralSource("ContactProfile");
    private GeneralSource contactBucket = new GeneralSource("ContactBucket");
    private GeneralSource source = new GeneralSource("ContactStats");

    private static int profileStep = 0;
    private static int bucketStep = 1;

    @Override
    @Test(groups = "functional")
    public void testTransformation() {
        prepareContact();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("ContactStatistics");
            configuration.setVersion(targetVersion);

            TransformationStepConfig profile = profile();
            TransformationStepConfig bucket = bucket();
            TransformationStepConfig calc = calcStats();
            // -----------
            List<TransformationStepConfig> steps = Arrays.asList( //
                    profile, //
                    bucket, //
                    calc //
            );
            // -----------
            steps.get(steps.size() - 1).setTargetSource(getTargetSourceName());
            configuration.setSteps(steps);
            return configuration;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected TransformationStepConfig profile() {
        TransformationStepConfig step = super.profile();
        step.setBaseSources(Collections.singletonList(contact.getSourceName()));
        step.setTargetSource(contactProfile.getSourceName());
        step.setConfiguration(setDataFlowEngine(step.getConfiguration(), "TEZ"));
        return step;
    }

    @Override
    protected ProfileConfig constructProfileConfig() {
        ProfileConfig config = new ProfileConfig();
        config.setEncAttrPrefix(CEAttr);
        return config;
    }

    @Override
    protected TransformationStepConfig bucket() {
        TransformationStepConfig step = super.bucket();
        step.setBaseSources(Collections.singletonList(contact.getSourceName()));
        step.setInputSteps(Collections.singletonList(profileStep));
        step.setTargetSource(contactBucket.getSourceName());
        step.setConfiguration(setDataFlowEngine(step.getConfiguration(), "TEZ"));
        return step;
    }

    @Override
    protected TransformationStepConfig calcStats() {
        TransformationStepConfig step = super.calcStats();
        step.setInputSteps(Arrays.asList(profileStep, bucketStep));
        CalculateStatsConfig config = new CalculateStatsConfig();
        config.setDedupFields(Arrays.asList("AccountId"));
        step.setConfiguration(setDataFlowEngine(JsonUtils.serialize(config), "FLINK"));
        step.setTargetSource(source.getSourceName());
        return step;
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    private void prepareContact() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("AccountId", String.class));
        columns.add(Pair.of("ContactId", String.class));
        columns.add(Pair.of("Title", String.class));

        Object[][] data = new Object[][] {
            { "Account1", "Contact1", "CEO" }, //
            { "Account1", "Contact2", "CEO" }, //
            { "Account2", "Contact3", "CEO" }, //
                { "Account2", "Contact4", "Manager" } //
        };

        uploadBaseSourceData(contact.getSourceName(), baseSourceVersion, columns, data);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        Object[][] expected = new Object[][] {
                { "Title", 3L, "2|1", "CEO|Manager" }, //
                { "AccountId", 2L, "1|1", "Account1|Account2" }, //
                { "ContactId", 4L, "1|1|1|1", "Contact4|Contact3|Contact2|Contact1" }, //
        };
        Map<String, Object[]> expectedMap = new HashMap<>();
        for (Object[] data : expected) {
            Object[] cnts = new Object[2];
            cnts[0] = data[1];
            Map<String, String> bktCnts = new HashMap<>();
            String[] bkts = ((String) data[3]).split("\\|");
            String[] bktsCnts = ((String) data[2]).split("\\|");
            for (int i = 0; i < bkts.length; i++) {
                bktCnts.put(bkts[i], bktsCnts[i]);
            }
            cnts[1] = bktCnts;
            expectedMap.put((String) data[0], cnts);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Assert.assertTrue(expectedMap.containsKey(record.get("AttrName").toString()));
            Object[] cnts = expectedMap.get(record.get("AttrName").toString());
            Assert.assertTrue(isObjEquals(record.get("AttrCount"), cnts[0]));
            Map<String, String> expBktCnts = (Map<String, String>) cnts[1];
            String[] bktsCnts = record.get("BktCounts").toString().split("\\|");
            CategoricalBucket algo = JsonUtils.deserialize(record.get("BktAlgo").toString(), CategoricalBucket.class);
            for (int i = 0; i < bktsCnts.length; i++) {
                String cnt = bktsCnts[i].split(":")[1];
                String bkt = algo.getCategories().get(i);
                Assert.assertTrue(expBktCnts.containsKey(bkt));
                Assert.assertEquals(cnt, expBktCnts.get(bkt));
            }
        }
    }
}
