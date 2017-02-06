package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterLookupRebuildConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;

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
        AccountMasterLookupRebuildConfig conf = new AccountMasterLookupRebuildConfig();
        conf.setCountryField("Country");
        conf.setDomainField("Domain");
        conf.setDomainMappingPrimaryDomainField("PrimaryDomain");
        conf.setDomainMappingSecondaryDomainField("SecondaryDomain");
        conf.setDuDunsField("LE_PRIMARY_DUNS");
        conf.setDunsField("DUNS");
        conf.setEmployeeField("EMPLOYEES_HERE");
        conf.setGuDunsField("GLOBAL_ULTIMATE_DUNS_NUMBER");
        conf.setIsPrimaryDomainField("LE_IS_PRIMARY_DOMAIN");
        conf.setIsPrimaryLocationField("LE_IS_PRIMARY_LOCATION");
        conf.setKeyField("Key");
        conf.setLatticeIdField("LatticeID");
        conf.setSalesVolumeUsDollars("SALES_VOLUME_US_DOLLARS");
        conf.setStateField("State");
        conf.setZipCodeField("ZipCode");
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
        Set<String> keys = new HashSet<String>();
        Object[][] expectedData = { { "_DOMAIN_a.com_DUNS_NULL", 6L }, { "_DOMAIN_secondary.com_DUNS_NULL", 6L },
                { "_DOMAIN_mailserver.com_DUNS_NULL", 6L },
                { "_DOMAIN_a.com_DUNS_NULL_COUNTRY_Country1_STATE_NULL_ZIPCODE_ZipCode1", 1L },
                { "_DOMAIN_secondary.com_DUNS_NULL_COUNTRY_Country1_STATE_NULL_ZIPCODE_ZipCode1", 1L },
                { "_DOMAIN_mailserver.com_DUNS_NULL_COUNTRY_Country1_STATE_NULL_ZIPCODE_ZipCode1", 1L },
                { "_DOMAIN_a.com_DUNS_NULL_COUNTRY_Country1_STATE_State1_ZIPCODE_NULL", 1L },
                { "_DOMAIN_secondary.com_DUNS_NULL_COUNTRY_Country1_STATE_State1_ZIPCODE_NULL", 1L },
                { "_DOMAIN_mailserver.com_DUNS_NULL_COUNTRY_Country1_STATE_State1_ZIPCODE_NULL", 1L },
                { "_DOMAIN_a.com_DUNS_NULL_COUNTRY_Country1_STATE_NULL_ZIPCODE_NULL", 1L },
                { "_DOMAIN_secondary.com_DUNS_NULL_COUNTRY_Country1_STATE_NULL_ZIPCODE_NULL", 1L },
                { "_DOMAIN_mailserver.com_DUNS_NULL_COUNTRY_Country1_STATE_NULL_ZIPCODE_NULL", 1L },
                { "_DOMAIN_a.com_DUNS_NULL_COUNTRY_Country2_STATE_NULL_ZIPCODE_NULL", 6L },
                { "_DOMAIN_secondary.com_DUNS_NULL_COUNTRY_Country2_STATE_NULL_ZIPCODE_NULL", 6L },
                { "_DOMAIN_mailserver.com_DUNS_NULL_COUNTRY_Country2_STATE_NULL_ZIPCODE_NULL", 6L },
                { "_DOMAIN_b.com_DUNS_NULL", 8L }, { "_DOMAIN_c.com_DUNS_NULL", 9L },
                { "_DOMAIN_d.com_DUNS_NULL", 10L }, { "_DOMAIN_NULL_DUNS_01", 10L }, { "_DOMAIN_NULL_DUNS_02", 2L },
                { "_DOMAIN_NULL_DUNS_03", 3L }, { "_DOMAIN_NULL_DUNS_04", 4L }, { "_DOMAIN_NULL_DUNS_05", 5L },
                { "_DOMAIN_NULL_DUNS_06", 6L }, { "_DOMAIN_NULL_DUNS_07", 8L }, { "_DOMAIN_a.com_DUNS_01", 1L },
                { "_DOMAIN_secondary.com_DUNS_01", 1L }, { "_DOMAIN_mailserver.com_DUNS_01", 1L },
                { "_DOMAIN_a.com_DUNS_02", 2L }, { "_DOMAIN_secondary.com_DUNS_02", 2L },
                { "_DOMAIN_mailserver.com_DUNS_02", 2L }, { "_DOMAIN_a.com_DUNS_03", 3L },
                { "_DOMAIN_secondary.com_DUNS_03", 3L }, { "_DOMAIN_mailserver.com_DUNS_03", 3L },
                { "_DOMAIN_a.com_DUNS_04", 4L }, { "_DOMAIN_secondary.com_DUNS_04", 4L },
                { "_DOMAIN_mailserver.com_DUNS_04", 4L }, { "_DOMAIN_a.com_DUNS_05", 5L },
                { "_DOMAIN_secondary.com_DUNS_05", 5L }, { "_DOMAIN_mailserver.com_DUNS_05", 5L },
                { "_DOMAIN_a.com_DUNS_06", 6L }, { "_DOMAIN_secondary.com_DUNS_06", 6L },
                { "_DOMAIN_mailserver.com_DUNS_06", 6L },
                { "_DOMAIN_b.com_DUNS_07", 8L }, { "_DOMAIN_d.com_DUNS_01", 10L }, };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            Long latticeId = (Long) record.get(LATTICEID);
            String key = String.valueOf(record.get(KEY));
            log.info(latticeId + " " + key);
            Assert.assertFalse(keys.contains(key));
            keys.add(key);
            boolean flag = false;
            for (Object[] data : expectedData) {
                if (key.equals(data[0]) && latticeId.equals(data[1])) {
                    flag = true;
                    break;
                }
            }
            Assert.assertTrue(flag);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 45);
    }

}
