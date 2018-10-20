package com.latticeengines.datacloud.etl.transformation.service.impl.filetosrc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.source.impl.OrbCompanyRaw;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.IngestedFileToSourceTransformer;
import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig.CompressType;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class OrbFileToSourceTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(OrbFileToSourceTestNG.class);

    @Autowired
    private OrbCompanyRaw source;

    @Autowired
    private IngestionSource baseSource;

    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        baseSource.setIngestionName(IngestionNames.ORB_INTELLIGENCE);
        uploadBaseSourceFile(baseSource, "orb-db2-export-sample.zip", baseSourceVersion);
        prepareUncompressDir();
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

        configuration.setName("OrbCompanyFileToSource");
        configuration.setVersion(targetVersion);

        // -----------
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add(baseSource.getSourceName());
        step1.setBaseSources(baseSources);
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
        conf.setIngestionName(IngestionNames.ORB_INTELLIGENCE);
        conf.setFileNameOrExtension("orb_companies.csv");
        conf.setCompressedFileNameOrExtension("orb-db2-export-sample.zip");
        conf.setCompressType(CompressType.ZIP);
        return JsonUtils.serialize(conf);
    }

    // To cover the case that uncompress folder already exists. Need to delete
    // first
    private void prepareUncompressDir() {
        String uncompressPath = hdfsPathBuilder.constructTransformationSourceDir(baseSource, baseSourceVersion)
                .append(EngineConstants.UNCOMPRESSED).toString();
        try {
            HdfsUtils.mkdir(yarnConfiguration, uncompressPath);
        } catch (IOException e) {
            throw new RuntimeException("Fail to create directory " + uncompressPath, e);
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        String[] expectedIds = new String[] { "13262799", "12221764", "51040422", "8129065", "11417478", "17145445",
                "17149076", "12438907", "8825824", "117155600", "126441554", "117155602", "117155604", "133303900",
                "136492131", "141925778", "142109806", "137396383", "118386803", "138412260", "109785854", "116109312",
                "113262801" };
        Set<String> expectedIdSet = new HashSet<>(Arrays.asList(expectedIds));
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String orbNum = record.get("OrbNum").toString();
            Assert.assertTrue(expectedIdSet.contains(orbNum));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 23);
    }

}
