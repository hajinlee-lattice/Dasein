package com.latticeengines.datacloud.etl.transformation.service.impl.seed;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.dataflow.transformation.seed.OrbCacheSeedMarkerFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedSecDomainRebuildConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.OrbCacheSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class OrbCacheSeedSecDomainTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(OrbCacheSeedSecDomainTestNG.class);

    private GeneralSource source = new GeneralSource("OrbCacheSeedSecondaryDomain");
    private GeneralSource baseSource = new GeneralSource("OrbCacheSeed");

    private static final String MARKER_FIELD_NAME = "_secondaryDomainMarker_";
    private static final String DOMAIN_FIELD_NAME = "Domain";

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
    protected String getTargetSourceName() {
        return source.getSourceName();
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
            step1.setTransformer(OrbCacheSeedMarkerFlow.TRANSFORMER);
            step1.setTargetSource("OrbCacheSeedMarked");
            step1.setConfiguration(getMarkerConfig());

            TransformationStepConfig step2 = new TransformationStepConfig();
            List<Integer> inputSteps = new ArrayList<Integer>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            step2.setTargetSource("OrbCacheSeedSecondaryDomain");
            step2.setTransformer("orbCacheSeedSecondaryDomainTransformer");
            step2.setConfiguration(getAccumulationConfig());
            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);
            // -----------
            configuration.setSteps(steps);

            configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    private String getAccumulationConfig() {
        OrbCacheSeedSecDomainRebuildConfig conf = new OrbCacheSeedSecDomainRebuildConfig();
        List<String> domainMappingFields = new ArrayList<>();
        domainMappingFields.add(DOMAIN_FIELD_NAME);
        domainMappingFields.add("PrimaryDomain");
        conf.setDomainMappingFields(domainMappingFields);
        conf.setMarkerFieldName(MARKER_FIELD_NAME);
        conf.setSecondaryDomainFieldName(DOMAIN_FIELD_NAME);
        conf.setRenamedSecondaryDomainFieldName("SecondaryDomain");
        return JsonUtils.serialize(conf);
    }

    private String getMarkerConfig() throws JsonProcessingException {
        OrbCacheSeedMarkerConfig conf = new OrbCacheSeedMarkerConfig();
        conf.setMarkerFieldName(MARKER_FIELD_NAME);
        List<String> fieldsToCheck = new ArrayList<>();
        fieldsToCheck.add("IsSecondaryDomain");
        fieldsToCheck.add("DomainHasEmail");
        conf.setFieldsToCheck(fieldsToCheck);
        return JsonUtils.serialize(conf);
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
                        hasFieldMismatchInRecord = true;
                        break;

                    }
                    idx++;

                }

                if (!hasFieldMismatchInRecord || idx == record.getSchema().getFields().size()) {
                    foundMatchingRecord = true;
                    break;
                }
            }
            if (!foundMatchingRecord) {
                log.info(record.toString());
            }
            Assert.assertTrue(foundMatchingRecord);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 2);
    }
}
