package com.latticeengines.datacloud.etl.transformation.service.impl.minidc;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.minidc.MiniAMDomainDunsFlow;
import com.latticeengines.datacloud.dataflow.transformation.minidc.MiniAMDomainDunsInitFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.minidc.MiniAMDomainDunsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.minidc.MiniAMDomainDunsInitConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.IterativeStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class MiniAMDomainDunsTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(MiniAMDomainDunsTestNG.class);

    private GeneralSource source = new GeneralSource("MiniAMDomainDuns");
    private GeneralSource baseSource = new GeneralSource("GoldenDataSet");
    private GeneralSource baseSourceAMS = new GeneralSource("AccountMasterSeed");
    private GeneralSource baseSourceDnBSeed = new GeneralSource("DnbSeed");

    @Test(groups = "pipeline1", enabled = true)
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
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("MiniAMDomainDuns");
        configuration.setVersion(targetVersion);

        // Initialize Golden Data Set
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSourcesStep1 = new ArrayList<>();
        baseSourcesStep1.add(baseSource.getSourceName());
        step1.setBaseSources(baseSourcesStep1);
        step1.setTargetVersion("2017-01-01_21-50-34_UTC");
        step1.setTargetSource(source.getSourceName());
        step1.setTransformer(MiniAMDomainDunsInitFlow.TRANSFORMER);
        String confParamStr1 = getMiniAMDomainDunsInitConfig();
        step1.setConfiguration(confParamStr1);

        // Initialize DnbSeed Data Set and AMSeed Data Set
        TransformationStepConfig step2 = new TransformationStepConfig();
        List<String> baseSourcesStep2 = new ArrayList<>();
        step2.setStepType(TransformationStepConfig.ITERATIVE);
        step2.setTargetSource(source.getSourceName());
        step2.setTransformer(MiniAMDomainDunsFlow.TRANSFORMER);
        String confParamStr2 = getMiniDnbAMDomainDunsConfig();
        step2.setConfiguration(confParamStr2);
        baseSourcesStep2.add(source.getSourceName());
        baseSourcesStep2.add(baseSourceDnBSeed.getSourceName());
        baseSourcesStep2.add(baseSourceAMS.getSourceName());
        step2.setBaseSources(baseSourcesStep2);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);
        steps.add(step2);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        int rowCount = 0;
        Object[] expectedData = new Object[] { "yahoo.com", "234567890", "adobe.com", "citrix.com", "google.com",
                "145789000", "345678911", "901234567", "snapchat.com", "microsoft.com", "oracle.com",
                "4567890123", "890123456", "987624588", "intel.com", "paypal.com", "salesforce.com",
                "909090909", "kaggle.com", "krux.com", "123456789", "amazon.com", "facebook.com", "visa.com",
                "333333333", "3456789012", "121459889", "192093993", "890898989" };
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
        Assert.assertEquals(rowCount, 29);
    }

    private void prepareGoldenDataSetSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("Name", String.class));
        columns.add(Pair.of("City", String.class));
        columns.add(Pair.of("State", String.class));
        columns.add(Pair.of("Country", String.class));
        Object[][] data = new Object[][] { { "google.com", "123456789", "Google", "Mountain View", "California", "United States"}, 
                { null, "234567890", "Apple", "Cupertino", "California", "United States" },
                { "intel.com", "3456789012", "Intel", "Santa Clara", "California", "United States" },
                { "facebook.com", "4567890123", "Facebook", "Menlo Park", "California", "United States" },
                { "amazon.com", "234567890", "Amazon", "Seattle", "Washington", "United States" },
                { "salesforce.com", null, "Salesforce", "San Francisco", "California", "United States" },
                { null, null, "Visa", "Foster city", "California", "United States" },
                { "citrix.com", null, "Citrix", "Santa Clara", "California", "United States" },
                { null, "987624588", "Nvedia", "Santa Clara", "California", "United States" },
                { "krux.com", "", "Krux", "San Francisco", "California", "United States" } };
        uploadBaseSourceData(baseSource.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareDnBDataSetSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("GU", String.class));
        columns.add(Pair.of("DU", String.class));
        Object[][] data = new Object[][] { { "kaggle.com", "123456789", null, "123456712" },
                { "visa.com", "234567890", "234567811", "234567812" },
                { "snapchat.com", "345678911", "345678912", "345678902" },
                { "yahoo.com", "890123456", "890123453", "890123452" },
                { "google.com", "901234567", null, "901234561" },
                { "adobe.com", "121459889", "345723848", "123456712" },
                { "paypal.com", "192093993", "234567811", "234567812" },
                { "oracle.com", "145789000", null, "121211212" }, { "datos.com", "910329039", null, null },
                { "microsoft.com", "890898989", "234567811", "234567811" },
                { "data.com", "787998900", "423500012", "234567811" }, { "yahoo.com", "121459889", null, null },
                { "citrix.com", null, "345678912", null }
        };

        uploadBaseSourceData(baseSourceDnBSeed.getSourceName(), baseSourceVersion, columns, data);
    }

    private void prepareAMDataSetSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("DOMAIN", String.class));
        columns.add(Pair.of("Duns", String.class));
        Object[][] data = new Object[][] { { "tesla", "111111111" }, { "yelp.com", "222222222" },
                { "intel.com", "333333333" }, { "netapp.com", "888888888" }, { "paypal.com", "145789000" },
                { "adobe.com", "909090909" }, { null, "191090190" }, { null, "799090909" },
                { "infotech.com", "901234561" }, { "kaggle.com", null }, { "snapchat.com", "121459889" } };
        uploadBaseSourceData(baseSourceAMS.getSourceName(), baseSourceVersion, columns, data);
    }

    private String getMiniAMDomainDunsInitConfig() {
        MiniAMDomainDunsInitConfig conf = new MiniAMDomainDunsInitConfig();
        // For storing DOMAINS of all golden data sets
        Map<String, String> goldenDomain = new HashMap<String, String>();
        goldenDomain.put(baseSource.getSourceName(), "Domain");
        // For storing DUNS of all golden data sets
        Map<String, String> goldenDuns = new HashMap<String, String>();
        goldenDuns.put(baseSource.getSourceName(), "DUNS");
        conf.setGoldenInputDataSetDomain(goldenDomain);
        conf.setGoldenInputDataSetDuns(goldenDuns);
        conf.setOutputDataSetType("Type");
        conf.setOutputDataSetValue("Value");
        return JsonUtils.serialize(conf);
    }

    private String getMiniDnbAMDomainDunsConfig() {
        MiniAMDomainDunsConfig conf = new MiniAMDomainDunsConfig();
        IterativeStepConfig.ConvergeOnCount iterateStrategy = new IterativeStepConfig.ConvergeOnCount();
        iterateStrategy.setIteratingSource(source.getSourceName());
        iterateStrategy.setCountDiff(0);
        conf.setIterateStrategy(iterateStrategy);
        // For storing DOMAINS of all seeds as {seed, domain_name}
        Map<String, String> domain = new HashMap<>();
        domain.put(baseSourceDnBSeed.getSourceName(), "Domain");
        domain.put(baseSourceAMS.getSourceName(), "DOMAIN");
        // For storing DUNS of all seeds as {seed, duns_name}
        Map<String, String> duns = new HashMap<>();
        duns.put(baseSourceDnBSeed.getSourceName(), "DUNS");
        duns.put(baseSourceAMS.getSourceName(), "Duns");
        conf.setDnbInputDataSetDomain("Domain");
        conf.setDnbInputDataSetDuns("DUNS");
        conf.setDnbInputDataSetGU("GU");
        conf.setDnbInputDataSetDU("DU");
        conf.setSeedInputDataSetDomain(domain);
        conf.setSeedInputDataSetDuns(duns);
        conf.setMiniInputDataSetType("Type");
        conf.setMiniInputDataSetValue("Value");
        conf.setOutputDataSetType("Type");
        conf.setOutputDataSetValue("Value");
        return JsonUtils.serialize(conf);
    }
}
