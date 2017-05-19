package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SourceDomainCleanupByDuTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMSeedFixDomainInDUTreeTestNG extends
        TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final String ACCOUNT_MASTER_SEED_CLEANED = "AccountMasterSeedCleaned";

    private static final Log log = LogFactory
            .getLog(AMSeedFixDomainInDUTreeTestNG.class);

    @Autowired
    PipelineSource source;

    @Autowired
    AccountMasterSeed baseAccountMasterSeedSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "MatchResult";
    String targetVersion;

    @Test(groups = "pipeline1")
    public void testTransformation() throws IOException {
        prepareAMSeed();
        String targetSourcePath = hdfsPathBuilder.podDir().append(ACCOUNT_MASTER_SEED_CLEANED).toString();
        HdfsUtils.rmdir(yarnConfiguration, targetSourcePath);

        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(baseAccountMasterSeedSource.getSourceName(), baseSourceVersion)
                .toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {

        PipelineTransformationRequest request = new PipelineTransformationRequest();

        request.setName("AccountMasterSeedCleanupPipeline");
        request.setVersion("2017-01-09_19-12-43_UTC");
        List<TransformationStepConfig> steps = new ArrayList<>();

        TransformationStepConfig step = new TransformationStepConfig();
        step.setBaseSources(Collections.singletonList("AccountMasterSeed"));
        step.setTransformer("sourceDomainCleanupByDuTransformer");
        step.setConfiguration(getCleanupByDuConfig());
        step.setTargetSource(ACCOUNT_MASTER_SEED_CLEANED);
        steps.add(step);

        request.setSteps(steps);
        PipelineTransformationConfiguration configuration = pipelineTransformationService
                .createTransformationConfiguration(request);
        String configJson =  JsonUtils.serialize(configuration);
        log.info("Transformation Cleanup Json=" + configJson);
        return configuration;
    }

    private String getCleanupByDuConfig() {
        SourceDomainCleanupByDuTransformerConfig config = new SourceDomainCleanupByDuTransformerConfig();
        config.setDuField("LE_PRIMARY_DUNS");
        config.setDunsField("DUNS");
        config.setDomainField("Domain");
        config.setAlexaRankField("AlexaRank");
        config.setIsPriDomField("LE_IS_PRIMARY_DOMAIN");
        return JsonUtils.serialize(config);
    }

    @Override
    protected String getPathForResult() {
        targetSourceName = ACCOUNT_MASTER_SEED_CLEANED;
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    private void prepareAMSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeID", Long.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("LE_IS_PRIMARY_DOMAIN", String.class));
        columns.add(Pair.of("LE_IS_PRIMARY_LOCATION", String.class));
        columns.add(Pair.of("LE_PRIMARY_DUNS", String.class));
        columns.add(Pair.of("AlexaRank", Integer.class));
        uploadBaseSourceData(baseAccountMasterSeedSource.getSourceName(), "2017-01-09_19-12-43_UTC", columns, amsData);
    }

    private Object[][] amsData = new Object[][] { //
            { 1L, "google.com", "01", "Y", "Y", "01", null }, //
            { 2L, null, "02", "N", "N", "01", null }, //
            { 3L, null, "03", "N", "Y", "01", null }, //
            { 4L, "facebook.com", "001", "N", "Y", "002", null }, //
            { 5L, null, "002", "N", "Y", "002", null }, //
            { 6L, "lattice.com", null, "N", "Y", "002", null }, //
            { 7L, "lattice.com", "004", "N", null, "002", null }, //
            { 8L, "lattice.com", "005", "Y", "Y", null, null }, //
            { 9L, null, "0003", "N", "Y", null, null }, //
            { 10L, "oracle.com", "07", "Y", "Y", "10", 19 }, //
            { 11L, "oracle1.com", "08", "Y", "Y", "10", 1 }, //
            { 12L, "oracle2.com", "09", "Y", "Y", "10", 8 }, //
            { 13L, "oracle2.com", "11", "Y", "Y", "10", 9 }, //
            { 14L, null, "11", "N", "Y", "10", 9 }, //
    };

    private Object[][] expectedData = new Object[][] { //
            { 1L, "google.com", "01", "Y", "Y", "01", null }, //
            { 2L, "google.com", "02", "Y", "N", "01", null }, // Populate domain = google.com, isPriDom = Y
            { 3L, "google.com", "03", "Y", "Y", "01", null }, // Populate domain = google.com, isPriDom = Y
            { 4L, "facebook.com", "001", "N", "Y", "002", null }, //
            { 5L, "lattice.com", "002", "Y", "Y", "002", null }, // Populate domain = lattice.com, isPriDom = Y
            { 6L, "lattice.com", null, "N", "Y", "002", null }, //
            { 7L, "lattice.com", "004", "N", null, "002", null }, //
            { 8L, "lattice.com", "005", "Y", "Y", null, null }, //
            { 9L, null, "0003", "N", "Y", null, null }, //
            { 10L, "oracle.com", "07", "Y", "Y", "10", 19 }, //
            { 11L, "oracle1.com", "08", "Y", "Y", "10", 1 }, //
            { 12L, "oracle2.com", "09", "Y", "Y", "10", 8 }, //
            { 13L, "oracle2.com", "11", "Y", "Y", "10", 9 }, //
            { 14L, "oracle1.com", "11", "Y", "Y", "10", 9 }, // Populate domain = oracle1.com, isPriDom = Y
    };

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
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
            log.info(record);
            Assert.assertEquals(record.get("Domain") == null ? null : record.get("Domain").toString(),
                    (String) data[1]);
            Assert.assertEquals(record.get("DUNS") == null ? null : record.get("DUNS").toString(), (String) data[2]);
            Assert.assertEquals(
                    record.get("LE_IS_PRIMARY_DOMAIN") == null ? null : record.get("LE_IS_PRIMARY_DOMAIN").toString(),
                    (String) data[3]);
        }
    }
}
