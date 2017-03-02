package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeedMerged;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.LatticeIdAssignConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

public class LatticeIdAssignServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(LatticeIdAssignServiceTestNG.class);

    @Autowired
    AccountMasterSeed source;

    @Autowired
    AccountMasterSeedMerged ams;

    GeneralSource amId = new GeneralSource("AccountMasterId");

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "AccountMasterSeed";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional")
    public void testTransformation() {
        prepareAMSeed();
        prepareAMId();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }


    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("LatticeIdAssign");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(ams.getSourceName());
            baseSources.add(amId.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer("latticeIdAssignTransformer");
            step1.setTargetSource(targetSourceName);
            String confParamStr1 = getTransformerConfig();
            step1.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getTransformerConfig() throws JsonProcessingException {
        LatticeIdAssignConfig conf = new LatticeIdAssignConfig();
        conf.setAmSeedIdField("LatticeID");
        conf.setAmSeedDunsField("DUNS");
        conf.setAmSeedDomainField("Domain");
        conf.setAmIdSrcIdField("LatticeID");
        conf.setAmIdSrcDunsField("DUNS");
        conf.setAmIdSrcDomainField("Domain");
        return om.writeValueAsString(conf);
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
    String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    private Object[][] amsData = new Object[][] { //
            { "dom1.com", "DUNS1" }, //
            { "dom1.com", "DUNS2" }, //
            { "dom11.com", "DUNS11" }, //
            { "dom11.com", "DUNS22" }, //
            { null, "DUNS2" }, //
            { null, "DUNS3" }, //
            { null, "DUNS22" }, //
            { null, "DUNS33" }, //
            { "dom2.com", "DUNS4" }, //
            { "dom3.com", "DUNS4" }, //
            { "dom22.com", "DUNS44" }, //
            { "dom33.com", "DUNS44" }, //
            { "dom3.com", null }, //
            { "dom33.com", null }, //
    };

    private void prepareAMSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        uploadBaseSourceData(ams.getSourceName(), baseSourceVersion, columns, amsData);
    }

    private Object[][] amIdData = new Object[][] { //
            { 10_000_000_001L, "dom1.com", "DUNS1" }, //
            { 10_000_000_002L, "dom1.com", "DUNS2" }, //
            { 20_000_000_003L, null, "DUNS2" }, //
            { 20_000_000_004L, null, "DUNS3" }, //
            { 30_000_000_001L, "dom2.com", "DUNS4" }, //
            { 30_000_000_002L, "dom3.com", "DUNS4" }, //
            { 40_000_000_001L, "dom3.com", null }, //
    };

    private void prepareAMId() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeID", Long.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        uploadBaseSourceData(amId.getSourceName(), baseSourceVersion, columns, amIdData);
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        int existingRowNum = 0;
        Set<Long> ids = new HashSet<Long>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Long id = (Long) record.get("LatticeID");
            Object duns = record.get("DUNS");
            if (duns instanceof Utf8) {
                duns = duns.toString();
            }
            Object domain = record.get("Domain");
            if (domain instanceof Utf8) {
                domain = domain.toString();
            }
            log.info(String.format("LatticeID = %d, Domain = %s, DUNS = %s", id, domain, duns));
            for (Object[] data : amIdData) {
                if (((domain == null && data[1] == null) || (domain != null && domain.equals(data[1])))
                        && ((duns == null && data[2] == null) || (duns != null && duns.equals(data[2])))) {
                    Assert.assertEquals(id, data[0]);
                    existingRowNum++;
                }
            }
            Assert.assertNotNull(id);
            Assert.assertTrue(!ids.contains(id));
            ids.add(id);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 14);
        Assert.assertEquals(existingRowNum, 7);
    }

}
