package com.latticeengines.datacloud.etl.transformation.service.impl.seed;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ORBSEC_ATTR_PRIDOM;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ORBSEC_ATTR_SECDOM;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.dataflow.transformation.seed.OrbCacheSeedMarkerFlow;
import com.latticeengines.datacloud.dataflow.transformation.seed.OrbCacheSeedSecDomainFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedSecDomainRebuildConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.OrbCacheSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

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
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

        // -----------
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add("OrbCacheSeed");
        step1.setBaseSources(baseSources);
        step1.setTransformer(OrbCacheSeedMarkerFlow.TRANSFORMER);
        step1.setTargetSource("OrbCacheSeedMarked");
        step1.setConfiguration(getMarkerConfig());

        TransformationStepConfig step2 = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(0);
        step2.setInputSteps(inputSteps);
        step2.setTargetSource(source.getSourceName());
        step2.setTransformer(OrbCacheSeedSecDomainFlow.TRANSFORMER);
        step2.setConfiguration(getAccumulationConfig());
        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);
        steps.add(step2);
        // -----------
        configuration.setSteps(steps);

        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));
        return configuration;
    }

    private String getAccumulationConfig() {
        OrbCacheSeedSecDomainRebuildConfig conf = new OrbCacheSeedSecDomainRebuildConfig();
        List<String> domainMappingFields = new ArrayList<>();
        domainMappingFields.add(DOMAIN_FIELD_NAME);
        domainMappingFields.add(ORBSEC_ATTR_PRIDOM);
        conf.setDomainMappingFields(domainMappingFields);
        conf.setMarkerFieldName(MARKER_FIELD_NAME);
        conf.setSecondaryDomainFieldName(DOMAIN_FIELD_NAME);
        conf.setRenamedSecondaryDomainFieldName(ORBSEC_ATTR_SECDOM);
        return JsonUtils.serialize(conf);
    }

    private String getMarkerConfig() {
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
        // SecondaryDomain, PrimaryDomain
        Object[][] expectedData = { //
                { "secondary.com", "a.com" }, //
                { "mailserver.com", "a.com" }//
        };
        Map<String, Object[]> expectedMap = Arrays.stream(expectedData)
                .collect(Collectors.toMap(x -> (String) x[1] + "_" + (String) x[0], x -> x));

        while (records.hasNext()) {
            GenericRecord record = records.next();
            String priDomain = record.get(ORBSEC_ATTR_PRIDOM).toString();
            String secDomain = record.get(ORBSEC_ATTR_SECDOM).toString();
            String key = priDomain + "_" + secDomain;
            Object[] expectedObject = expectedMap.get(key);
            Assert.assertNotNull(expectedObject);
            Assert.assertTrue(isObjEquals(record.get(ORBSEC_ATTR_SECDOM), expectedObject[0]));
            Assert.assertTrue(isObjEquals(record.get(ORBSEC_ATTR_PRIDOM), expectedObject[1]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 2);
    }
}
