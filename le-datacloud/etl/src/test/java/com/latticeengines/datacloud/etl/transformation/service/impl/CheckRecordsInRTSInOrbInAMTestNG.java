package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.RowsInRTSInOrbInAMFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.RowsInRTSInOrbInAMConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class CheckRecordsInRTSInOrbInAMTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    GeneralSource source = new GeneralSource("RecordsInRTSInAMInOrb");
    GeneralSource baseSource1 = new GeneralSource("AccountMasterSeed");
    GeneralSource baseSource2 = new GeneralSource("OrbCacheSeedSecondaryDomain");
    GeneralSource baseSource3 = new GeneralSource("OrbCacheSeedStandard");
    private static final Logger log = LoggerFactory.getLogger(CheckRecordsInRTSInOrbInAMTestNG.class);

    String targetSourceName = "RecordsInRTSInAMInOrb";

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareOrbSeedSecondaryDom();
        prepareOrbSeedStandard();
        prepareAmSeed();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private void prepareAmSeed() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("DomainSource", String.class));
        schema.add(Pair.of("LatticeID", Long.class));
        schema.add(Pair.of("DUNS", String.class));
        schema.add(Pair.of("Domain", String.class));
        Object[][] data = new Object[][] { { "DnB", 1001L, "DUNS10", "netflix.com" },
                { "RTS", 1002L, "DUNS11", "netapp.com" }, { "RTS", 1003L, "DUNS12", null },
                { "RTS", 1004L, "DUNS13", "apple.com" }, { "RTS", 1005L, "DUNS14", "" },
                { "RTS", 1006L, "DUNS15", "craigslist.com" }, { "RTS", 1007L, "DUNS16", "target.com" },
                { "RTS", 1008L, "DUNS17", "payless.com" }, { "RTS", 1009L, "DUNS18", "amazon.com" },
                { "DnB", 1010L, "DUNS19", "macys.com" } };
        uploadBaseSourceData(baseSource1.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareOrbSeedSecondaryDom() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("SecondaryDomain", String.class));
        schema.add(Pair.of("PrimaryDomain", String.class));
        Object[][] data = new Object[][] { { "netflix.com", "datalab.com" }, { "netapp.com", "purestorage.com" },
                { "datos.com", "intuit.com" }, { "apple.com", "uber.com" }, { "ms.com", "lyft.com" },
                { "craigslist.com", "netapp.com" }, { "target.com", "macys.com" }, { "payless.com", "oldnavy.com" },
                { "amazon.com", "walmart.com" }, { "macys.com", "payless.com" } };
        uploadBaseSourceData(baseSource2.getSourceName(), baseSourceVersion, schema, data);
    }

    private void prepareOrbSeedStandard() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("DUNS", String.class));
        schema.add(Pair.of("Domain", String.class));
        Object[][] data = new Object[][] { { "DUNS10", "netflix.com" }, { "DUNS11", "netapp.com" },
                { "DUNS12", "datos.com" }, { "DUNS13", "apple.com" }, { "DUNS14", "ms.com" },
                { "DUNS15", "craigslist.com" }, { "DUNS16", "target.com" }, { "DUNS17", "payless.com" },
                { "DUNS18", "amazon.com" }, { "DUNS19", "macys.com" } };
        uploadBaseSourceData(baseSource3.getSourceName(), baseSourceVersion, schema, data);
    }

    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("CheckRecordsInRTS");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourceStep = new ArrayList<String>();
            baseSourceStep.add(baseSource1.getSourceName());
            baseSourceStep.add(baseSource2.getSourceName());
            baseSourceStep.add(baseSource3.getSourceName());
            step1.setBaseSources(baseSourceStep);
            step1.setTransformer(RowsInRTSInOrbInAMFlow.TRANSFORMER_NAME);
            String confParamStr1 = getRTSInOrbInAMCheck();
            step1.setConfiguration(confParamStr1);
            step1.setTargetSource(source.getSourceName());

            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            configuration.setSteps(steps);
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getRTSInOrbInAMCheck() throws JsonProcessingException {
        RowsInRTSInOrbInAMConfig conf = new RowsInRTSInOrbInAMConfig();
        conf.setDomainSource("DomainSource");
        conf.setDomain("Domain");
        conf.setSecDomain("SecondaryDomain");
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    Object[][] expectedDataValues = new Object[][] { //
            { "RTS", "DUNS13", 1004L, "apple.com" }, //
            { "RTS", "DUNS11", 1002L, "netapp.com" }, //
            { "RTS", "DUNS16", 1007L, "target.com" }, //
            { "RTS", "DUNS18", 1009L, "amazon.com" }, //
            { "RTS", "DUNS15", 1006L, "craigslist.com" }, //
            { "RTS", "DUNS17", 1008L, "payless.com" } };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Map<String, Object[]> expectedData = new HashMap<>();
        for (Object[] data : expectedDataValues) {
            expectedData.put(String.valueOf(data[1]) + (String.valueOf(data[2])) + (String.valueOf(data[3])), data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String duns = String.valueOf(record.get(1));
            String latId = String.valueOf(record.get(2));
            String dom = String.valueOf(record.get(3));
            Object[] expected = expectedData.get(duns + latId + dom);
            Assert.assertTrue(isObjEquals(record.get(0), expected[0]));
            Assert.assertTrue(isObjEquals(record.get(1), expected[1]));
            Assert.assertTrue(isObjEquals(record.get(2), expected[2]));
            Assert.assertTrue(isObjEquals(record.get(3), expected[3]));
            rowCount++;
            log.info("record : " + record);

        }
        Assert.assertEquals(rowCount, 6);
    }

}
