package com.latticeengines.datacloud.etl.transformation.service.impl.filetosrc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import com.latticeengines.datacloud.etl.transformation.transformer.impl.source.IngestedFileToSourceTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.IngestedFileToSourceTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.IngestedFileToSourceTransformerConfig.CompressType;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceIngestion;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DnBFileToSourceTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DnBFileToSourceTestNG.class);

    private GeneralSource source = new GeneralSource("DnBCacheSeedRaw");

    private IngestionSource baseSource = new IngestionSource(IngestionNames.DNB_CASHESEED);

    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_052.OUT.gz", baseSourceVersion);
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_053.OUT.gz", baseSourceVersion);
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_054.OUT.gz", baseSourceVersion);
        // 051 is a corrupted version to test skipping failure
        uploadBaseSourceFile(baseSource, "LE_SEED_OUTPUT_2017_01_051.OUT.gz", baseSourceVersion);
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

        configuration.setName("DnBFileToSource");
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
        conf.setFileNameOrExtension(".OUT");
        conf.setCompressedFileNameOrExtension(".OUT.gz");
        conf.setCompressType(CompressType.GZ);
        conf.setDelimiter("|");
        conf.setQualifier(null);
        conf.setCharset("ISO-8859-1");
        return JsonUtils.serialize(conf);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        String[] expectedDuns = { "260014358", "260014362", "260014403", "260014417", "260014421",
                "260014484", "260014491", "260014542", "260014594", "260014625", "260014635",
                "260014637", "260014707", "260014742", "260014794", "260014834", "260014903",
                "260015074", "260014336", "260014359", "260014361", "260015096", "260015108",
                "260015159", "260014363", "260014374", "260014404", "260015163", "260015171" };
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
        Assert.assertEquals(rowNum, 57);
    }
}
