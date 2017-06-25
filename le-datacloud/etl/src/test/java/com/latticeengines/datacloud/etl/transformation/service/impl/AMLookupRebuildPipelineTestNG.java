package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Comparator;
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
import com.latticeengines.datacloud.core.source.impl.AccountMasterLookup;
import com.latticeengines.datacloud.dataflow.transformation.AMLookupRebuild;
import com.latticeengines.datacloud.dataflow.transformation.AMSeedSecondDomainCleanup;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMSeedSecondDomainCleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterLookupRebuildConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;


public class AMLookupRebuildPipelineTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(AMLookupRebuildPipelineTestNG.class);

    private static final String LATTICEID = "LatticeID";
    private static final String KEY = "Key";

    @Autowired
    AccountMasterLookup source;

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    private String baseSourceAccountMasterSeed = "AccountMasterSeed";
    private String baseSourceOrbCacheSeedSecondaryDomain = "OrbCacheSeedSecondaryDomain";
    private String targetSeedName = "AccountMasterSeedCleaned";
    private String targetSourceName = "AccountMasterLookup";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareAMSeed();
        prepareOrbSeed();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            configuration.setName("AccountMasterLookupRebuild");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSourceAccountMasterSeed);
            baseSources.add(baseSourceOrbCacheSeedSecondaryDomain);
            step1.setBaseSources(baseSources);
            step1.setTransformer(AMSeedSecondDomainCleanup.TRANSFORMER_NAME);
            step1.setTargetSource(targetSeedName);
            String confParamStr1 = getCleanupTransformerConfig();
            step1.setConfiguration(confParamStr1);

            // -----------
            TransformationStepConfig step2 = new TransformationStepConfig();
            baseSources = new ArrayList<String>();
            baseSources.add(targetSeedName);
            baseSources.add(baseSourceOrbCacheSeedSecondaryDomain);
            step2.setBaseSources(baseSources);
            step2.setTransformer(AMLookupRebuild.TRANSFORMER_NAME);
            step2.setTargetSource(targetSourceName);
            String confParamStr2 = getTransformerConfig();
            step2.setConfiguration(confParamStr2);

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

    private String getCleanupTransformerConfig() throws JsonProcessingException {
        AMSeedSecondDomainCleanupConfig conf = new AMSeedSecondDomainCleanupConfig();
        conf.setDomainField("Domain");
        conf.setSecondDomainField("SecondaryDomain");
        conf.setDunsField("DUNS");
        return om.writeValueAsString(conf);
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
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    private void prepareAMSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("LatticeID", Long.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("State", String.class));
        columns.add(Pair.of("ZipCode", String.class));
        columns.add(Pair.of("Country", String.class));
        columns.add(Pair.of("DUNS", String.class));
        columns.add(Pair.of("LE_IS_PRIMARY_LOCATION", String.class));
        columns.add(Pair.of("LE_IS_PRIMARY_DOMAIN", String.class));
        columns.add(Pair.of("LE_PRIMARY_DUNS", String.class));
        columns.add(Pair.of("GLOBAL_ULTIMATE_DUNS_NUMBER", String.class));
        columns.add(Pair.of("EMPLOYEES_HERE", Long.class));
        columns.add(Pair.of("SALES_VOLUME_US_DOLLARS", Long.class));

        Object[][] data = new Object[][] {
                // all kinds of keys
                { 1L, "dom1.com", null, null, null, "DUNS1", "Y", "Y", "Y", "DUNS1", 10000L, 10000L },
                { 2L, "dom2.com", null, null, "Country1", "DUNS2", "Y", "Y", "Y", "DUNS2", 10000L, 10000L },
                { 3L, "dom3.com", null, "ZipCode3", "Country3", "DUNS3", "Y", "Y", "Y", "DUNS3", 10000L, 10000L },
                { 4L, "dom4.com", "State4", null, "Country4", "DUNS4", "Y", "Y", "Y", "DUNS4", 10000L, 10000L },
                { 5L, "dom5.com", "State5", "ZipCode5", "Country5", "DUNS5", "Y", "Y", "Y", "DUNS4", 10000L, 10000L },

                // secondary domain not exists
                { 11L, "dom11.com", null, null, null, "DUNS11", "Y", "Y", "Y", "DUNS11", 10000L, 10000L },
                { 12L, "dom12.com", null, null, null, null, "Y", "Y", "Y", "DUNS11", 10000L, 10000L },

                // secondary domain exists with DUNS
                { 21L, "dom21.com", null, null, null, "DUNS21", "Y", "Y", "Y", "DUNS21", 10000L, 10000L },
                { 22L, "dom22.com", null, null, null, "DUNS22", "Y", "Y", "Y", "DUNS22", 10000L, 10000L },

                // secondary domain exists without DUNS
                { 31L, "dom31.com", null, null, null, "DUNS31", "Y", "Y", "Y", "DUNS31", 10000L, 10000L },
                { 32L, "dom32.com", null, null, null, null, "Y", "Y", "Y", null, 10000L, 10000L }
        };

        uploadBaseSourceData(baseSourceAccountMasterSeed, baseSourceVersion, columns, data);
    }

    private void prepareOrbSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("PrimaryDomain", String.class));
        columns.add(Pair.of("SecondaryDomain", String.class));

        Object[][] data = new Object[][] {
                { "dom11.com", "dom13.com" },
                { "dom21.com", "dom22.com" },
                { "dom31.com", "dom32.com" }
        };

        uploadBaseSourceData(baseSourceOrbCacheSeedSecondaryDomain, baseSourceVersion, columns, data);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");

        Object[][] expectedData = {
                { "_DOMAIN_dom1.com_DUNS_NULL", 1L }, //
                { "_DOMAIN_dom1.com_DUNS_DUNS1", 1L }, //
                { "_DOMAIN_NULL_DUNS_DUNS1", 1L }, //

                { "_DOMAIN_dom2.com_DUNS_NULL", 2L }, //
                { "_DOMAIN_dom2.com_DUNS_DUNS2", 2L }, //
                { "_DOMAIN_NULL_DUNS_DUNS2", 2L }, //
                { "_DOMAIN_dom2.com_DUNS_NULL_COUNTRY_Country1_STATE_NULL_ZIPCODE_NULL", 2L }, //

                { "_DOMAIN_dom3.com_DUNS_NULL_COUNTRY_Country3_STATE_NULL_ZIPCODE_ZipCode3", 3L }, //
                { "_DOMAIN_dom3.com_DUNS_DUNS3", 3L }, //
                { "_DOMAIN_dom3.com_DUNS_NULL", 3L }, //
                { "_DOMAIN_NULL_DUNS_DUNS3", 3L }, //
                { "_DOMAIN_dom3.com_DUNS_NULL_COUNTRY_Country3_STATE_NULL_ZIPCODE_NULL", 3L }, //

                { "_DOMAIN_NULL_DUNS_DUNS4", 4L }, //
                { "_DOMAIN_dom4.com_DUNS_NULL_COUNTRY_Country4_STATE_State4_ZIPCODE_NULL", 4L }, //
                { "_DOMAIN_dom4.com_DUNS_NULL", 4L }, //
                { "_DOMAIN_dom4.com_DUNS_DUNS4", 4L }, //
                { "_DOMAIN_dom4.com_DUNS_NULL_COUNTRY_Country4_STATE_NULL_ZIPCODE_NULL", 4L }, //

                { "_DOMAIN_dom5.com_DUNS_NULL_COUNTRY_Country5_STATE_State5_ZIPCODE_NULL", 5L }, //
                { "_DOMAIN_dom5.com_DUNS_NULL_COUNTRY_Country5_STATE_NULL_ZIPCODE_NULL", 5L }, //
                { "_DOMAIN_dom5.com_DUNS_DUNS5", 5L }, //
                { "_DOMAIN_dom5.com_DUNS_NULL_COUNTRY_Country5_STATE_NULL_ZIPCODE_ZipCode5", 5L }, //
                { "_DOMAIN_NULL_DUNS_DUNS5", 5L }, //
                { "_DOMAIN_dom5.com_DUNS_NULL", 5L }, //

                { "_DOMAIN_dom11.com_DUNS_DUNS11", 11L }, //
                { "_DOMAIN_NULL_DUNS_DUNS11", 11L }, //
                { "_DOMAIN_dom11.com_DUNS_NULL", 11L }, //

                { "_DOMAIN_dom12.com_DUNS_NULL", 12L }, //

                { "_DOMAIN_dom13.com_DUNS_DUNS11", 11L }, //
                { "_DOMAIN_dom13.com_DUNS_NULL", 11L }, //

                { "_DOMAIN_dom21.com_DUNS_DUNS21", 21L }, //
                { "_DOMAIN_dom21.com_DUNS_NULL", 21L }, //
                { "_DOMAIN_NULL_DUNS_DUNS21", 21L }, //

                { "_DOMAIN_dom22.com_DUNS_DUNS22", 22L }, //
                { "_DOMAIN_NULL_DUNS_DUNS22", 22L }, //
                { "_DOMAIN_dom22.com_DUNS_NULL", 22L }, //

                { "_DOMAIN_dom31.com_DUNS_DUNS31", 31L }, //
                { "_DOMAIN_dom31.com_DUNS_NULL", 31L }, //
                { "_DOMAIN_NULL_DUNS_DUNS31", 31L }, //

                { "_DOMAIN_dom32.com_DUNS_DUNS31", 31L }, //
                { "_DOMAIN_dom32.com_DUNS_NULL", 31L } //
        };

        Map<String, Long> lookup = new HashMap<>();
        for (Object[] row: expectedData) {
            lookup.put((String) row[0], (Long) row[1]);
        }

        List<GenericRecord> sorted = new ArrayList<>();
        records.forEachRemaining(sorted::add);
        sorted.sort(Comparator.comparing(r -> ((Long) r.get("LatticeID"))));

        Set<String> seenKeys = new HashSet<>();
        sorted.forEach(record -> {
            Long latticeId = (Long) record.get(LATTICEID);
            String key = String.valueOf(record.get(KEY));
            log.info(latticeId + " " + key);
            Assert.assertTrue(lookup.containsKey(key));
            seenKeys.add(key);
            Long expectedId = lookup.get(key);
            Assert.assertEquals(latticeId, expectedId);
        });

        Assert.assertEquals(seenKeys.size(), lookup.size());
    }

}
