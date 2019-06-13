package com.latticeengines.datacloud.etl.transformation.service.impl.filetosrc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.IngestedFileToSourceTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.IngestedFileToSourceTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.IngestedFileToSourceTransformerConfig.CompressType;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceIngestion;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class HGDataFileToSourceTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(OrbFileToSourceTestNG.class);

    private GeneralSource source = new GeneralSource("HGSeedRaw");

    private IngestionSource baseSource = new IngestionSource(IngestionNames.HGDATA);

    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, "Lattice_Engines_2017-02-14.zip", baseSourceVersion);
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

        configuration.setName("HGDataFileToSource");
        configuration.setVersion(targetVersion);

        // -----------
        TransformationStepConfig step1 = new TransformationStepConfig();
        step1.setBaseSources(Arrays.asList(baseSource.getSourceName()));
        step1.setBaseIngestions(Collections.singletonMap(baseSource.getSourceName(),
                new SourceIngestion(baseSource.getIngestionName())));
        step1.setTransformer(IngestedFileToSourceTransformer.TRANSFORMER_NAME);
        step1.setTargetSource(source.getSourceName());
        String confParamStr1 = getIngestedFileToSourceTransformerConfig();
        step1.setConfiguration(confParamStr1);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private String getIngestedFileToSourceTransformerConfig() {
        IngestedFileToSourceTransformerConfig conf = new IngestedFileToSourceTransformerConfig();
        conf.setFileNameOrExtension("7330 Lattice Engines.csv");
        conf.setCompressedFileNameOrExtension("Lattice_Engines_2017-02-14.zip");
        conf.setCompressType(CompressType.ZIP);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        String[][] expectedData = new String[][] { { "gersonco.com", "Louis M. Gerson Co. Inc." },
                { null, "Boston Steel Fabricators Inc." }, { "eckelacoustic.com", "Eckel Industries" },
                { "nationalnonwovens.com", "National Nonwovens" }, { null, "Banc One Capital Markets" },
                { "byersimports.com", "Byers Imports" } };
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Object domain = record.get("URL");
            if (domain instanceof Utf8) {
                domain = domain.toString();
            }
            Object company = record.get("Company");
            if (company instanceof Utf8) {
                company = company.toString();
            }
            log.info(String.format("URL = %s, Company = %s", String.valueOf(domain), String.valueOf(company)));
            boolean flag = false;
            for (String[] data : expectedData) {
                if (((domain == null && data[0] == null) || (domain != null && domain.equals(data[0])))
                        && ((company == null && data[1] == null) || (company != null && company.equals(data[1])))) {
                    flag = true;
                    break;
                }
            }
            Assert.assertTrue(flag);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 9);
    }
}
