package com.latticeengines.datacloud.etl.transformation.service.impl.ams;

import java.io.IOException;
import java.util.ArrayList;
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
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;

public class AMSeedFixDomainInDUTreeTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AMSeedFixDomainInDUTreeTestNG.class);

    private GeneralSource baseSource = new GeneralSource("AMSeed");
    private GeneralSource source = new GeneralSource("AMSeedClean");

    @Test(groups = "pipeline1")
    public void testTransformation() throws IOException {
        prepareAMSeed();

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {

        PipelineTransformationRequest request = new PipelineTransformationRequest();

        request.setName("AccountMasterSeedCleanupPipeline");
        List<TransformationStepConfig> steps = new ArrayList<>();

        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Collections.singletonList(baseSource.getSourceName()));
        step.setTransformer(DataCloudConstants.TRANSFORMER_AMS_FILL_DOM_IN_DU);
        step.setConfiguration("{}");
        step.setTargetSource(source.getSourceName());
        steps.add(step);

        request.setSteps(steps);
        PipelineTransformationConfiguration configuration = pipelineTransformationService
                .createTransformationConfiguration(request);
        String configJson =  JsonUtils.serialize(configuration);
        return configuration;
    }

    private void prepareAMSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(DataCloudConstants.LATTICE_ID, Long.class));
        columns.add(Pair.of(DataCloudConstants.AMS_ATTR_DOMAIN, String.class));
        columns.add(Pair.of(DataCloudConstants.AMS_ATTR_DUNS, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_DU_DUNS, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_ALEXA_RANK, Integer.class));
        // To test whether original operation logs could still be maintained
        columns.add(Pair.of(OperationLogUtils.DEFAULT_FIELD_NAME, String.class));
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, amsData);
    }

    // LatticeID, Domain, DUNS, LE_IS_PRIMARY_DOMAIN, LE_IS_PRIMARY_LOCATION,
    // LE_PRIMARY_DUNS, AlexaRank, LE_OperationLogs
    private Object[][] amsData = new Object[][] { //
            // Case 1: Choose google.com as DU primary domain due to it's DU
            // domain
            { 1L, "google.com", "01", "Y", "Y", "01", null, null }, //
            { 2L, null, "02", "N", "N", "01", null, null }, //
            { 3L, null, "03", "N", "Y", "01", null, null }, //
            // Case 2: Choose lattice.com as DU primary domain due to it has
            // highest occurrence
            { 4L, "facebook.com", "001", "N", "Y", "002", null, "[TestLogs]" }, //
            { 5L, null, "002", "N", "Y", "002", null, "[TestLogs]" }, //
            { 6L, "lattice.com", null, "N", "Y", "002", null, "[TestLogs]" }, //
            { 7L, "lattice.com", "004", "N", null, "002", null, "[TestLogs]" }, //
            { 8L, "lattice.com", "005", "Y", "Y", null, null, "[TestLogs]" }, //
            // Case 3: Empty DUDuns
            { 9L, null, "0003", "N", "Y", null, null, null }, //
            // Case 4: Choose oracle1.com as DU primary domain due to lowest
            // AlexaRank
            { 10L, "oracle.com", "07", "Y", "Y", "10", 19, null }, //
            { 11L, "oracle1.com", "08", "Y", "Y", "10", 1, null }, //
            { 12L, "oracle2.com", "09", "Y", "Y", "10", 8, null }, //
            { 13L, "oracle2.com", "11", "Y", "Y", "10", 9, null }, //
            { 14L, null, "11", "N", "Y", "10", 9, null }, //
    };

    // LatticeID, Domain, DUNS, LE_IS_PRIMARY_DOMAIN, LE_IS_PRIMARY_LOCATION,
    // LE_PRIMARY_DUNS, AlexaRank, LE_OperationLogs
    private Object[][] expectedData = new Object[][] { //
            // Case 1
            { 1L, "google.com", "01", "Y", "Y", "01", null, null }, //
            // Populate domain = google.com, isPriDom = Y
            { 2L, "google.com", "02", "Y", "N", "01", null,
                    "[Step=AMSeedFillDomainInDU,Code=FILL_DOM_IN_DU,Log=DU domain]" },
            { 3L, "google.com", "03", "Y", "Y", "01", null,
                    "[Step=AMSeedFillDomainInDU,Code=FILL_DOM_IN_DU,Log=DU domain]" },

            // Case 2
            { 4L, "facebook.com", "001", "N", "Y", "002", null, "[TestLogs]" }, //
            // Populate domain = lattice.com, isPriDom = Y
            { 6L, "lattice.com", null, "N", "Y", "002", null, "[TestLogs]" }, //
            { 5L, "lattice.com", "002", "Y", "Y", "002", null,
                    "[TestLogs],[Step=AMSeedFillDomainInDU,Code=FILL_DOM_IN_DU,Log=Higher occurrence=2]" },
            { 7L, "lattice.com", "004", "N", null, "002", null, "[TestLogs]" }, //
            { 8L, "lattice.com", "005", "Y", "Y", null, null, "[TestLogs]" }, //

            // Case 3
            { 9L, null, "0003", "N", "Y", null, null, null }, //

            // Case 4
            { 10L, "oracle.com", "07", "Y", "Y", "10", 19, null }, //
            { 11L, "oracle1.com", "08", "Y", "Y", "10", 1, null }, //
            { 12L, "oracle2.com", "09", "Y", "Y", "10", 8, null }, //
            { 13L, "oracle2.com", "11", "Y", "Y", "10", 9, null }, //
            // Populate domain = oracle1.com, isPriDom = Y
            { 14L, "oracle1.com", "11", "Y", "Y", "10", 9,
                    "[Step=AMSeedFillDomainInDU,Code=FILL_DOM_IN_DU,Log=Lower AlexaRank=1]" }, //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        Map<Long, GenericRecord> recordMap = new HashMap<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Long id = (Long) record.get("LatticeID");
            recordMap.put(id, record);
            rowNum++;
        }
        
        log.info("Total result records " + rowNum);
        Assert.assertEquals(rowNum, 14);
        for (Object[] data : expectedData) {
            GenericRecord record = recordMap.get((Long) data[0]);
            Assert.assertNotNull(record);
            log.info(record.toString());
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.AMS_ATTR_DOMAIN), data[1]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.AMS_ATTR_DUNS), data[2]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN), data[3]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION), data[4]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.ATTR_DU_DUNS), data[5]));
            Assert.assertTrue(isObjEquals(record.get(DataCloudConstants.ATTR_ALEXA_RANK), data[6]));
            Assert.assertTrue(isObjEquals(record.get(OperationLogUtils.DEFAULT_FIELD_NAME), data[7]));
        }
    }
}
