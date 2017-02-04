package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMasterLookup;
import com.latticeengines.datacloud.core.source.impl.AccountMasterSeed;
import com.latticeengines.datacloud.core.source.impl.OrbCacheSeedSecondaryDomain;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationStepConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public class AccountMasterLookupRebuildServiceImplTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(AccountMasterLookupRebuildServiceImplTestNG.class);

    private static final String LATTICEID = "LatticeID";
    private static final String KEY = "Key";

    @Autowired
    AccountMasterLookup source;

    @Autowired
    AccountMasterSeed baseSourceAccountMasterSeed;

    @Autowired
    OrbCacheSeedSecondaryDomain baseSourceOrbCacheSeedSecondaryDomain;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "AccountMasterLookup";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional")
    public void testTransformation() {
        uploadBaseSourceFile(baseSourceAccountMasterSeed, "AccountMasterSeed_TestAccountMasterLookup",
                baseSourceVersion);
        uploadBaseSourceFile(baseSourceOrbCacheSeedSecondaryDomain,
                "OrbCacheSeedSecondaryDomain_TestAccountMasterLookup", baseSourceVersion);
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

            configuration.setName("AccountMasterLookupRebuild");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add("AccountMasterSeed");
            baseSources.add("OrbCacheSeedSecondaryDomain");
            step1.setBaseSources(baseSources);
            step1.setTransformer("accountMasterLookupRebuildTransformer");
            step1.setTargetSource(targetSourceName);
            String confParamStr1 = getTransformerConfig();
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

    private String getTransformerConfig() throws JsonProcessingException {
        TransformerConfig conf = new TransformerConfig();
        conf.setTransformer("accountMasterLookupRebuildTransformer");
        return om.writeValueAsString(conf);
    }

    @Override
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Long latticeId = (Long) record.get(LATTICEID);
            String key = String.valueOf(record.get(KEY));
            log.info(latticeId + " " + key);

            Assert.assertTrue((latticeId.equals(1L) && key.equals("_DOMAIN_a.com_DUNS_NULL"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_secondary.com_DUNS_NULL"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_mailserver.com_DUNS_NULL"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_a.com_DUNS_01"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_mailserver.com_DUNS_01"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_secondary.com_DUNS_01"))
                    || (latticeId.equals(1L) && key.equals("_DOMAIN_NULL_DUNS_01"))
                    || (latticeId.equals(2L) && key.equals("_DOMAIN_secondary.com_DUNS_02"))
                    || (latticeId.equals(2L) && key.equals("_DOMAIN_NULL_DUNS_02"))
                    || (latticeId.equals(2L) && key.equals("_DOMAIN_a.com_DUNS_02"))
                    || (latticeId.equals(2L) && key.equals("_DOMAIN_mailserver.com_DUNS_02"))
                    || (latticeId.equals(3L) && key.equals("_DOMAIN_secondary.com_DUNS_03"))
                    || (latticeId.equals(3L) && key.equals("_DOMAIN_mailserver.com_DUNS_03"))
                    || (latticeId.equals(3L) && key.equals("_DOMAIN_NULL_DUNS_03"))
                    || (latticeId.equals(3L) && key.equals("_DOMAIN_a.com_DUNS_03"))
                    || (latticeId.equals(4L) && key.equals("_DOMAIN_b.com_DUNS_01"))
                    || (latticeId.equals(5L) && key.equals("_DOMAIN_b.com_DUNS_02"))
                    || (latticeId.equals(5L) && key.equals("_DOMAIN_b.com_DUNS_NULL"))
                    || (latticeId.equals(6L) && key.equals("_DOMAIN_b.com_DUNS_03"))
                    || (latticeId.equals(7L) && key.equals("_DOMAIN_NULL_DUNS_04"))
                    || (latticeId.equals(7L) && key.equals("_DOMAIN_b.com_DUNS_04"))
                    || (latticeId.equals(8L) && key.equals("_DOMAIN_c.com_DUNS_NULL")));
            rowNum++;
        }
        Assert.assertEquals(rowNum, 22);
    }

}
