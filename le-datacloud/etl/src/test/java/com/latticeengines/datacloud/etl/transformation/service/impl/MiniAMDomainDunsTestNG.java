package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
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
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MiniAMDomainDunsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.MiniAMDomainDunsInitConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class MiniAMDomainDunsTestNG extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(MiniAMDomainDunsTestNG.class);

    GeneralSource source = new GeneralSource("MiniAMDomainDuns");

    GeneralSource baseSource = new GeneralSource("GoldenDataSet");
    GeneralSource baseSourceAccountMasterSeed = new GeneralSource("AccountMasterSeed");
    GeneralSource baseSourceDnbSeed = new GeneralSource("DnbSeed");

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "MiniAMDomainDuns";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        prepareGoldenDataSetSeed();
        prepareAMDataSetSeed();
        prepareDnBDataSetSeed();
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
            configuration.setName("MiniAMDomainDuns");
            configuration.setVersion(targetVersion);

            // Initialize Golden Data Set
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSourcesStep1 = new ArrayList<String>();
            baseSourcesStep1.add(baseSource.getSourceName());
            step1.setBaseSources(baseSourcesStep1);
            step1.setTransformer("miniDnbAMDomainDunsTransformer");
            String confParamStr1 = getMiniAMDomainDunsInitConfig();
            step1.setConfiguration(confParamStr1);

            // Initialize DnbSeed Data Set and AMSeed Data Set
            TransformationStepConfig step2 = new TransformationStepConfig();
            List<String> baseSourcesStep2 = new ArrayList<String>();
            List<Integer> inputSteps = new ArrayList<Integer>();
            inputSteps.add(0);
            step2.setInputSteps(inputSteps);
            step2.setTargetSource(targetSourceName);
            step2.setTransformer("miniAMDomainDunsTransformer");
            String confParamStr2 = getMiniDnbAMDomainDunsConfig();
            step2.setConfiguration(confParamStr2);
            baseSourcesStep2.add(baseSourceDnbSeed.getSourceName());
            baseSourcesStep2.add(baseSourceAccountMasterSeed.getSourceName());
            step2.setBaseSources(baseSourcesStep2);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
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

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Object[] expectedData = new Object[] { "dom1.com", "123456789", "dom2.com", "234567890", "dom3.com",
                "3456789012", "dom4.com", "4567890123", "dom5.com", "6789012345", "111111111", "222222222",
                "333333333" };
        Set<Object> expectedSet = new HashSet<Object>(Arrays.asList(expectedData));
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Object type = record.get("Type");
            Object value = record.get("Value");
            Assert.assertTrue(expectedSet.contains(value.toString()));
            if (expectedSet.contains(value.toString())) {
                expectedSet.remove(value.toString());
            }
            type = type.toString();
            value = value.toString();
            log.info("Type : " + type + "Value : " + value);
            rowCount++;
        }
        Assert.assertEquals(rowCount, 13);
    }

    private void prepareGoldenDataSetSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        Object[][] data = new Object[][] { { "dom1.com", "123456789" }, { "dom2.com", "234567890" },
                { "dom3.com", "3456789012" }, { "dom4.com", "4567890123" }, { "dom5.com", "234567890" },
                { "dom3.com", "6789012345" } };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareDnBDataSetSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("GU", String.class));
        columns.add(Pair.of("DU", String.class));
        Object[][] data = new Object[][] { { "dom1.com", "123456789", null, "123456712" },
                { "dom2.com", "234567890", "234567811", "234567812" },
                { "dom3.com", "3456789012", "3456789011", "3456789022" },
                { "dom21.com", "8901234567", "8901234533", "8901234522" },
                { "dom22.com", "9012345678", null, "9012345611" } };

        uploadBaseSourceData(baseSourceDnbSeed.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareAMDataSetSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("DOMAIN", String.class));
        columns.add(Pair.of("Duns", String.class));
        Object[][] data = new Object[][] { { "dom1.com", "111111111" }, { "dom2.com", "222222222" },
                { "dom3.com", "333333333" }, { "dom44.com", "888888888" } };
        uploadBaseSourceData(baseSourceAccountMasterSeed.getSourceName(), baseSourceVersion, columns, data);
    }

    private String getMiniAMDomainDunsInitConfig() throws JsonProcessingException {
        MiniAMDomainDunsInitConfig conf = new MiniAMDomainDunsInitConfig();
        conf.setGoldenInputDataSetDomain("Domain");
        conf.setGoldenInputDataSetDuns("DUNS");
        conf.setOutputDataSetType("Type");
        conf.setOutputDataSetValue("Value");
        return om.writeValueAsString(conf);
    }

    private String getMiniDnbAMDomainDunsConfig() throws JsonProcessingException {
        MiniAMDomainDunsConfig conf = new MiniAMDomainDunsConfig();
        // For storing DOMAINS of all seeds as {seed, domain_name}
        Map<String, String> domain = new HashMap<String, String>();
        domain.put("DnbSeed", "Domain");
        domain.put("AccountMasterSeed", "DOMAIN");
        // For storing DUNS of all seeds as {seed, duns_name}
        Map<String, String> duns = new HashMap<String, String>();
        duns.put("DnbSeed", "DUNS");
        duns.put("AccountMasterSeed", "Duns");
        conf.setDnbInputDataSetDomain("Domain");
        conf.setDnbInputDataSetDuns("DUNS");
        conf.setDnbInputDataSetGU("GU");
        conf.setDnbInputDataSetDU("DU");
        conf.setSeedInputDataSetDomain(domain);
        conf.setSeedInputDataSetDuns(duns);
        conf.setMiniInputDataSetDomain("Type");
        conf.setMiniInputDataSetDuns("Value");
        conf.setOutputDataSetType("Type");
        conf.setOutputDataSetValue("Value");
        return om.writeValueAsString(conf);
    }

}
