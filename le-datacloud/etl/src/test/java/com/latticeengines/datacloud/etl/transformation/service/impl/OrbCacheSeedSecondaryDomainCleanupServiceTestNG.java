package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.OrbCacheSeed;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedSecondaryDomainAccumulationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedSecondaryDomainCleanupTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedSecondaryDomainMarkerTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class OrbCacheSeedSecondaryDomainCleanupServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(OrbCacheSeedSecondaryDomainCleanupServiceTestNG.class);

    @Autowired
    PipelineSource source;

    @Autowired
    OrbCacheSeed baseSource;

    String targetSourceName = "OrbCacheSeedSecondaryDomain";
    String targetVersion;

    private static final String MARKER_FIELD_NAME = "_secondaryDomainMarker_";
    private static final String DOMAIN_FIELD_NAME = "Domain";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, "OrbCacheSeed_Test", "2017-01-09_19-12-43_UTC");
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
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
        return hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("OrbCacheSeed");
            step1.setBaseSources(baseSources);
            step1.setTransformer("orbCacheSeedMarkerTransformer");
            step1.setTargetSource("OrbCacheSeedMarked");
            String confParamStr1 = getMarkerConfig();
            step1.setConfiguration(confParamStr1);
            // // -----------
            // TransformationStepConfig step2 = new TransformationStepConfig();
            // List<Integer> inputSteps = new ArrayList<Integer>();
            // inputSteps.add(0);
            // step2.setInputSteps(inputSteps);
            // step2.setTargetSource("OrbCacheSeedCleaned");
            // step2.setTransformer("orbCacheSeedCleanedTransformer");
            //
            // String confParamStr2 = getCleanupConfig();
            //
            // step2.setConfiguration(confParamStr2);
            // -----------
            TransformationStepConfig step3 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<Integer>();
            inputSteps.add(0);
            step3.setInputSteps(inputSteps);
            step3.setTargetSource("OrbCacheSeedSecondaryDomain");
            step3.setTransformer("orbCacheSeedSecondaryDomainTransformer");

            String confParamStr3 = getAccumulationConfig();

            step3.setConfiguration(confParamStr3);
            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            // steps.add(step2);
            steps.add(step3);
            // -----------
            configuration.setSteps(steps);

            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            System.out.println(om.writeValueAsString(configuration));
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    @SuppressWarnings("unused")
    private String getCleanupConfig() {
        OrbCacheSeedSecondaryDomainCleanupTransformerConfig confParam = new OrbCacheSeedSecondaryDomainCleanupTransformerConfig();
        confParam.setMarkerFieldName(MARKER_FIELD_NAME);
        String confParamStr = null;
        try {
            confParamStr = om.writeValueAsString(confParam);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return confParamStr;
    }

    private String getAccumulationConfig() {
        OrbCacheSeedSecondaryDomainAccumulationTransformerConfig confParam = new OrbCacheSeedSecondaryDomainAccumulationTransformerConfig();
        List<String> domainMappingFields = new ArrayList<>();
        domainMappingFields.add(DOMAIN_FIELD_NAME);
        domainMappingFields.add("PrimaryDomain");
        confParam.setDomainMappingFields(domainMappingFields);
        confParam.setMarkerFieldName(MARKER_FIELD_NAME);
        confParam.setSecondaryDomainFieldName(DOMAIN_FIELD_NAME);
        confParam.setRenamedSecondaryDomainFieldName("SecondaryDomain");
        String confParamStr = null;
        try {
            confParamStr = om.writeValueAsString(confParam);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return confParamStr;
    }

    private String getMarkerConfig() throws JsonProcessingException {
        OrbCacheSeedSecondaryDomainMarkerTransformerConfig conf = new OrbCacheSeedSecondaryDomainMarkerTransformerConfig();
        conf.setMarkerFieldName(MARKER_FIELD_NAME);
        List<String> fieldsToCheck = new ArrayList<>();
        fieldsToCheck.add("IsSecondaryDomain");
        fieldsToCheck.add("DomainHasEmail");
        conf.setFieldsToCheck(fieldsToCheck);
        return om.writeValueAsString(conf);
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSource.getSourceName(), targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        Object[][] expectedData = { //
                { "secondary.com", "a.com" }, //
                { "mailserver.com", "a.com" }//
        };

        while (records.hasNext()) {
            GenericRecord record = records.next();
            System.out.println();

            for (Field field : record.getSchema().getFields()) {
                if (record.get(field.name()) == null) {
                    System.out.print(", null");
                } else if (record.get(field.name()) instanceof Long) {
                    System.out.print(", " + record.get(field.name()) + "L");
                } else if (record.get(field.name()) instanceof Utf8) {
                    String txt = ((Utf8) record.get(field.name())).toString();
                    txt = txt.substring(0, (txt.length() < 40 ? txt.length() : 40));
                    System.out.print(", \"" + txt + "\"");
                } else {
                    throw new RuntimeException(record.get(field.name()).getClass().getName());
                }

            }

            boolean foundMatchingRecord = false;
            for (Object[] data : expectedData) {
                int idx = 0;
                boolean hasFieldMismatchInRecord = false;
                for (Field field : record.getSchema().getFields()) {
                    Object val = record.get(field.name());
                    if (val instanceof Utf8) {
                        val = ((Utf8) val).toString();
                    }
                    Object expectedVal = data[idx];
                    if ((val == null && expectedVal != null) //
                            || (val != null && !val.equals(expectedVal))) {

                        // System.out.print("[" + val + " - " +
                        // expectedVal + "], ");
                        hasFieldMismatchInRecord = true;
                        break;

                    }
                    idx++;

                }

                if (!hasFieldMismatchInRecord //
                        || idx == record.getSchema().getFields().size()) {
                    foundMatchingRecord = true;
                    break;
                }
            }
            if (!foundMatchingRecord) {
                System.out.println("\n\n================" + rowNum);
                for (Field field : record.getSchema().getFields()) {
                    if (record.get(field.name()) == null) {
                        System.out.print(", null");
                    } else if (record.get(field.name()) instanceof Long) {
                        System.out.print(", " + record.get(field.name()) + "L");
                    } else if (record.get(field.name()) instanceof Utf8) {
                        String txt = ((Utf8) record.get(field.name())).toString();
                        txt = txt.substring(0, (txt.length() < 40 ? txt.length() : 40));
                        System.out.print(", \"" + txt + "\"");
                    } else {
                        throw new RuntimeException(record.get(field.name()).getClass().getName());
                    }

                }
                System.out.println("================\n");
            }
            Assert.assertTrue(foundMatchingRecord);

            rowNum++;
        }
        System.out.println();
        Assert.assertEquals(rowNum, 2);
    }
}
