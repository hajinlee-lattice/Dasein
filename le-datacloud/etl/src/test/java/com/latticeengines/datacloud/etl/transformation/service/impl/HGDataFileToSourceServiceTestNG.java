package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.HGDataRaw;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig.CompressType;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

public class HGDataFileToSourceServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(OrbFileToSourceServiceTestNG.class);

    @Autowired
    HGDataRaw source;

    @Autowired
    IngestionSource baseSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "HGDataRaw";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        baseSource.setIngetionName(IngestionNames.HGDATA);
        uploadBaseSourceFile(baseSource, "Lattice_Engines_2017-02-14.zip", baseSourceVersion);
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
    String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            configuration.setName("HGDataFileToSource");
            configuration.setVersion(targetVersion);

            // -----------
            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("IngestionSource");
            step1.setBaseSources(baseSources);
            step1.setTransformer("ingestedFileToSourceTransformer");
            step1.setTargetSource(targetSourceName);
            String confParamStr1 = getIngestedFileToSourceTransformerConfig();
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

    private String getIngestedFileToSourceTransformerConfig() throws JsonProcessingException {
        IngestedFileToSourceTransformerConfig conf = new IngestedFileToSourceTransformerConfig();
        conf.setIngetionName(IngestionNames.HGDATA);
        conf.setFileNameOrExtension("7330 Lattice Engines.csv");
        conf.setCompressedFileNameOrExtension("Lattice_Engines_2017-02-14.zip");
        conf.setCompressType(CompressType.ZIP);
        return om.writeValueAsString(conf);
    }

    @Override
    String getPathForResult() {
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
        return hdfsPathBuilder.constructTransformationSourceDir(source, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
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
