package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import com.latticeengines.datacloud.core.source.impl.DnBCacheSeedRaw;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig.CompressType;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

import edu.emory.mathcs.backport.java.util.Arrays;

public class DnBFileToSourceServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(DnBFileToSourceServiceTestNG.class);

    @Autowired
    DnBCacheSeedRaw source;

    @Autowired
    IngestionSource baseSource;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "DnBCacheSeedRaw";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional", enabled = true)
    public void testTransformation() {
        baseSource.setIngestionName(IngestionNames.DNB_CASHESEED);
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_052.OUT.gz", baseSourceVersion);
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_053.OUT.gz", baseSourceVersion);
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_054.OUT.gz", baseSourceVersion);
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

            configuration.setName("DnBFileToSource");
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
        conf.setIngestionName(IngestionNames.DNB_CASHESEED);
        conf.setFileNameOrExtension(".OUT");
        conf.setCompressedFileNameOrExtension(".OUT.gz");
        conf.setCompressType(CompressType.GZ);
        conf.setDelimiter("|");
        conf.setQualifier(null);
        conf.setCharset("ISO-8859-1");
        return om.writeValueAsString(conf);
    }

    @Override
    String getPathForResult() {
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
        return hdfsPathBuilder.constructTransformationSourceDir(source, targetVersion).toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        String[] expectedDuns = { "970372808", "970372809", "970372810", "970372811", "970372812", "970372813",
                "970372814", "970372815", "970372816" };
        Set<String> set = new HashSet<>(Arrays.asList(expectedDuns));
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Object duns = record.get("DUNS_NUMBER");
            if (duns instanceof Utf8) {
                duns = duns.toString();
            }
            Assert.assertTrue(set.contains(duns));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 27);
    }
}
