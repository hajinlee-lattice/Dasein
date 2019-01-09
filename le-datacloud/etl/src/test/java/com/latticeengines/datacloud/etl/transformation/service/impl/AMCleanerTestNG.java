package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AMCleaner;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AMCleanerTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(AMCleanerTestNG.class);
    private GeneralSource baseSourceAccMaster1 = new GeneralSource("AccountMaster");
    private GeneralSource accMasterCleaned = new GeneralSource("AccountMasterCleaned");
    private GeneralSource baseSourceAccMaster2 = new GeneralSource("AccountMasterVerify");
    private GeneralSource source = new GeneralSource("AccountMasterVerifySource");
    private static final String DATA_CLOUD_VERSION = "2.0.6";

    @Autowired
    private SourceAttributeEntityMgr srcAttrEntityMgr;

    @Autowired
    private AccountMaster am;

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

    @Test(groups = "pipeline1")
    public void testTransformation() throws Exception {
        prepareAM();
        uploadBaseSourceFile(baseSourceAccMaster2.getSourceName(), "AccountMaster206",
                baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmIntermediateSource(accMasterCleaned, null);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private void prepareAM() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of("HGData_SegmentTechIndicators", String.class)); // Retain
        schema.add(Pair.of("BmbrSurge_BucketCode", String.class)); // Retain
        schema.add(Pair.of("CRMAlert", String.class)); // Boolean
        schema.add(Pair.of("AlexaCARank", String.class)); // Integer
        schema.add(Pair.of("BmbrSurge_CompositeScore", String.class)); // Retain
        schema.add(Pair.of("HGData_SupplierTechIndicators", String.class)); // Retain
        schema.add(Pair.of("BuiltWith_TechIndicators", String.class)); // Retain
        schema.add(Pair.of("AlexaAUPageViews", Integer.class)); // Double
        schema.add(Pair.of("LatticeID", Long.class)); // create another column LatticeAccountId and cast Long to String
        schema.add(Pair.of("BmbrSurge_Intent", String.class)); // Retain
        schema.add(Pair.of("AlexaDomains", String.class)); // String
        schema.add(Pair.of("AlexaOnlineSince", Integer.class)); // Long
        schema.add(Pair.of("IsMatched", Boolean.class)); // Drop
        schema.add(Pair.of("IsPublicDomain", Boolean.class)); // Drop
        schema.add(Pair.of("ExtraColumn", String.class)); // not present in source attribute, should be dropped
        uploadBaseSourceData(am.getSourceName(), baseSourceVersion, schema, amData);
        try {
            extractSchema(am, baseSourceVersion,
                    hdfsPathBuilder.constructSnapshotDir(am.getSourceName(), baseSourceVersion).toString());
        } catch (Exception e) {
            log.error(String.format("Fail to extract schema for source %s at version %s", am.getSourceName(),
                    baseSourceVersion));
        }
    }

    private Object[][] expectedData = new Object[][] {
            // HGData_SegmentTechIndicators,BmbrSurge_BucketCode,CRMAlert,AlexaCARank,BmbrSurge_CompositeScore,HGData_SupplierTechIndicators,BuiltWith_TechIndicators,AlexaAUPageViews,LatticeID,BmbrSurge_Intent,AlexaDomains,AlexaOnlineSince,LatticeAccountId
            { "ABC", "123", false, 2927, "AJDAK", "ABAB", "DEF", 78.0, 71L, "GHI", "wcmh4.com, wnbc.com", 871876800L,
                    "0000000000071" },
            { null, "AGAAJB", true, 114, null, "AAAAF", "HIJ", 0.0, 72L, "KLM", null, 1318318981L, "0000000000072" },
            { null, null, true, 128, null, "AAABBB", null, 99.0, 73L, "ANDKNS", null, 928491289L, "0000000000073" },
            { "JJAD", null, true, 1389, "AHBDKAN", null, "12313142", 198.0, 74L, null, "ghi.com", 248914897L,
                    "0000000000074" },
            // empty string attribute value replace with null
            { "     ", null, false, 11, null, null, null, 22.0, 87L, null, null, 312492849L, "0000000000087" }
    };

    private Object[][] amData = new Object[][] {
            // HGData_SegmentTechIndicators,BmbrSurge_BucketCode,CRMAlert,AlexaCARank,BmbrSurge_CompositeScore,HGData_SupplierTechIndicators,BuiltWith_TechIndicators,AlexaAUPageViews,LatticeID,BmbrSurge_Intent,AlexaDomains,AlexaOnlineSince,IsMatched,IsPublicDomain,ExtraColumn
            { "ABC", "123", "0", "2927", "AJDAK", "ABAB", "DEF", 78, 71L, "GHI", "wcmh4.com, wnbc.com", 871876800, true,
                    false, "abc" },
            { null, "AGAAJB", "1", "0114", null, "AAAAF", "HIJ", 0, 72L, "KLM", null, 1318318981, false, true, "def" },
            { null, null, "Y", "128", null, "AAABBB", null, 99, 73L, "ANDKNS", null, 928491289, true, true, "ghi" },
            { "JJAD", null, "TRUE", "1389", "AHBDKAN", null, "12313142", 198, 74L, null, "ghi.com", 248914897, false,
                    false, "jkl" },
            // empty input value for string attributes
            { "     ", "", "false", "11", "", "", null, 22, 87L, "", "", 312492849, false, true, "" }
    };

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("AMCleaner");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourcesStep1 = new ArrayList<>();
            baseSourcesStep1.add(baseSourceAccMaster1.getSourceName());
            step1.setBaseSources(baseSourcesStep1);
            step1.setTransformer(AMCleaner.TRANSFORMER_NAME);
            step1.setTargetSource(accMasterCleaned.getSourceName());
            String confParamStr1 = getAMCleanerConfig();
            step1.setConfiguration(confParamStr1);

            TransformationStepConfig step2 = new TransformationStepConfig();
            List<String> baseSourcesStep2 = new ArrayList<>();
            baseSourcesStep2.add(baseSourceAccMaster2.getSourceName());
            step2.setBaseSources(baseSourcesStep2);
            step2.setTransformer(AMCleaner.TRANSFORMER_NAME);
            step2.setConfiguration(getAMCleanerConfig());
            step2.setTargetSource(source.getSourceName());

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(step1);
            steps.add(step2);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getAMCleanerConfig() throws JsonProcessingException {
        AMCleanerConfig conf = new AMCleanerConfig();
        conf.setDataCloudVersion(DATA_CLOUD_VERSION);
        conf.setIsMini(true);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        String[] inputData = { "HGData_SegmentTechIndicators", "BmbrSurge_BucketCode", "CRMAlert", "AlexaCARank",
                "BmbrSurge_CompositeScore", "HGData_SupplierTechIndicators", "BuiltWith_TechIndicators",
                "AlexaAUPageViews", "LatticeID", "BmbrSurge_Intent", "AlexaDomains", "AlexaOnlineSince",
                "LatticeAccountId" };
        // verifying the row content
        Map<Object, Object[]> expectedMap = new HashMap<>();
        for (Object[] data : expectedData) {
            expectedMap.put(data[8], data);
        }
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Object[] expectedResult = expectedMap.get(record.get("LatticeID"));
            for (int i = 0; i < inputData.length; i++) {
                Assert.assertTrue(isObjEquals(record.get(inputData[i]), expectedResult[i]));
            }
            Assert.assertNull(record.getSchema().getField("IsMatched"));
            Assert.assertNull(record.getSchema().getField("IsPublicDomain"));
            Assert.assertNull(record.getSchema().getField("ExtraColumn"));
            rowNum++;
        }
        Assert.assertEquals(rowNum, expectedData.length);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
            GenericRecord record = records.next();
        List<Field> amfields = record.getSchema().getFields();
        // set of field and its type
        Map<String, String> mapFieldType = new HashMap<>();
        for (Field field : amfields) {
            String schemaType = field.schema().getTypes().get(0).getType().name();
            if (schemaType.equals("INT")) {
                schemaType = "INTEGER";
            }
            mapFieldType.put(field.name(), schemaType);
        }
        List<SourceAttribute> srcAttrs = srcAttrEntityMgr.getAttributes(AMCleaner.ACCOUNT_MASTER, AMCleaner.CLEAN,
                AMCleaner.TRANSFORMER_NAME, DATA_CLOUD_VERSION, false);
        int count = 0;
        for (SourceAttribute srcAttr : srcAttrs) {
            if (!srcAttr.getArguments().equals(("DROP"))) {
                // counting total attributes retained
                count++;
                // verifying the presence of required attribute
                if (srcAttr.getArguments().equals(("RETAIN")) || srcAttr.getArguments().equals(("LATTICEID"))) {
                    Assert.assertTrue(mapFieldType.containsKey(srcAttr.getAttribute()));
                } else if (mapFieldType.containsKey(srcAttr.getAttribute())) {
                    // verifying type of the argument
                    Assert.assertEquals(mapFieldType.get(srcAttr.getAttribute()), srcAttr.getArguments());
                }
            } else { // verifying columns which need to be dropped are really
                     // dropped
                Assert.assertTrue(!mapFieldType.containsKey(srcAttr.getAttribute()));
            }
        }
        Assert.assertEquals(amfields.size(), count);
    }

}
