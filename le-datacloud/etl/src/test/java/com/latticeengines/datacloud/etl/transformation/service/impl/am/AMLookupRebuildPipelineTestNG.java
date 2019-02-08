package com.latticeengines.datacloud.etl.transformation.service.impl.am;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.impl.AccountMasterLookup;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.AMSeedSecondDomainCleanup;
import com.latticeengines.datacloud.dataflow.transformation.am.AMLookupRebuild;
import com.latticeengines.datacloud.dataflow.transformation.ams.AMSeedPriActFix;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.am.AMLookupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMSeedSecondDomainCleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.dataflow.operations.OperationCode;
import com.latticeengines.domain.exposed.dataflow.operations.OperationLogUtils;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;


public class AMLookupRebuildPipelineTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AMLookupRebuildPipelineTestNG.class);

    private static final String LATTICEID = "LatticeID";
    private static final String KEY = "Key";

    @Autowired
    private AccountMasterLookup source;

    private String ams = "AccountMasterSeed";
    private String orbSecDom = "OrbCacheSeedSecondaryDomain";
    private String targetSeedName = "AccountMasterSeedCleaned";
    private String targetSourceName = "AccountMasterLookup";

    @Test(groups = "pipeline1")
    public void testTransformation() {
        prepareAMSeed();
        prepareOrbSeed();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmIntermediateSource(new GeneralSource(targetSeedName), null);
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

        configuration.setName("AccountMasterLookupRebuild");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add(ams);
        baseSources.add(orbSecDom);
        step1.setBaseSources(baseSources);
        step1.setTransformer(AMSeedSecondDomainCleanup.TRANSFORMER_NAME);
        String confParamStr1 = getCleanupTransformerConfig();
        step1.setConfiguration(confParamStr1);

        // -----------
        TransformationStepConfig step2 = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(0);
        step2.setInputSteps(inputSteps);
        step2.setTransformer(AMSeedPriActFix.TRANSFORMER_NAME);
        step2.setConfiguration("{}");
        step2.setTargetSource(targetSeedName);

        // -----------
        TransformationStepConfig step3 = new TransformationStepConfig();
        baseSources = new ArrayList<String>();
        baseSources.add(targetSeedName);
        baseSources.add(orbSecDom);
        step3.setBaseSources(baseSources);
        step3.setTransformer(AMLookupRebuild.TRANSFORMER_NAME);
        step3.setTargetSource(targetSourceName);
        String confParamStr3 = getAMLookupRebuildConfig();
        step3.setConfiguration(confParamStr3);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private String getCleanupTransformerConfig() {
        AMSeedSecondDomainCleanupConfig conf = new AMSeedSecondDomainCleanupConfig();
        conf.setDomainField("Domain");
        conf.setSecondDomainField("SecondaryDomain");
        conf.setDunsField("DUNS");
        return JsonUtils.serialize(conf);
    }

    private String getAMLookupRebuildConfig() {
        AMLookupConfig conf = new AMLookupConfig();
        conf.setCountryField("Country");
        conf.setDomainField("Domain");
        conf.setDomainMappingPrimaryDomainField("PrimaryDomain");
        conf.setDomainMappingSecondaryDomainField("SecondaryDomain");
        conf.setDunsField("DUNS");
        conf.setIsPrimaryDomainField("LE_IS_PRIMARY_DOMAIN");
        conf.setIsPrimaryLocationField("LE_IS_PRIMARY_LOCATION");
        conf.setKeyField("Key");
        conf.setLatticeIdField("LatticeID");
        conf.setStateField("State");
        conf.setZipCodeField("ZipCode");
        return JsonUtils.serialize(conf);
    }

    private void prepareAMSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(DataCloudConstants.LATTIC_ID, Long.class));
        columns.add(Pair.of(DataCloudConstants.AMS_ATTR_DOMAIN, String.class));
        columns.add(Pair.of(DataCloudConstants.AMS_ATTR_STATE, String.class));
        columns.add(Pair.of(DataCloudConstants.AMS_ATTR_ZIP, String.class));
        columns.add(Pair.of(DataCloudConstants.AMS_ATTR_COUNTRY, String.class));
        columns.add(Pair.of(DataCloudConstants.AMS_ATTR_DUNS, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_IS_PRIMARY_LOCATION, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_DU_DUNS, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_GU_DUNS, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_EMPLOYEE_HERE, Integer.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_SALES_VOL_US, Long.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_IS_PRIMARY_ACCOUNT, String.class));

        // LatticeID, Domain, State, ZipCode, Country, DUNS,
        // LE_IS_PRIMARY_LOCATION, LE_IS_PRIMARY_DOMAIN, LE_PRIMARY_DUNS,
        // GLOBAL_ULTIMATE_DUNS_NUMBER
        // LE_Is_Primary_Location, EMPLOYEES_HERE, SALES_VOLUME_US_DOLLARS,
        // IsPrimaryAccount
        Object[][] data = new Object[][] {
                // all kinds of keys
                { 1L, "dom1.com", null, null, null, "DUNS1", "Y", "Y", null, "DUNS1", 10000, 10000L, null },
                { 2L, "dom2.com", null, null, "Country1", "DUNS2", "Y", "Y", null, "DUNS2", 10000, 10000L, null },
                { 3L, "dom3.com", null, "ZipCode3", "Country3", "DUNS3", "Y", "Y", null, "DUNS3", 10000, 10000L, null },
                { 4L, "dom4.com", "State4", null, "Country4", "DUNS4", "Y", "Y", null, "DUNS4", 10000, 10000L, null },
                { 5L, "dom5.com", "State5", "ZipCode5", "Country5", "DUNS5", "Y", "Y", null, "DUNS4", 10000, 10000L,
                        null },

                // secondary domain not exists
                { 11L, "dom11.com", null, null, null, "DUNS11", "Y", "Y", null, "DUNS11", 10000, 10000L, null },
                { 12L, "dom12.com", null, null, null, null, "Y", "Y", null, "DUNS11", 10000, 10000L, null },

                // secondary domain exists with DUNS
                { 21L, "dom21.com", null, null, null, "DUNS21", "Y", "Y", null, "DUNS21", 10000, 10000L, null },
                { 22L, "dom22.com", null, null, null, "DUNS22", "Y", "Y", null, "DUNS22", 10000, 10000L, null },

                // secondary domain exists without DUNS
                { 31L, "dom31.com", null, null, null, "DUNS31", "Y", "Y", null, "DUNS31", 10000, 10000L, null },
                { 32L, "dom32.com", null, null, null, null, "Y", "Y", null, null, 10000, 10000L, null },

                // test priority to pick primary location
                // test IsPrimaryAccount
                { 33L, "dom001.com", null, null, null, "DUNS001", "N", "Y", null, null, null, null, "Y" },
                { 34L, "dom001.com", null, null, null, "DUNS002", "Y", "Y", null, null, null, null, "N" },
                // test DUNS not empty
                { 35L, "dom003.com", null, null, null, null, "Y", "Y", null, null, null, null, null },
                { 36L, "dom003.com", null, null, null, "DUNS003", "N", "Y", null, null, null, null, null },
                // test DUDuns not empty
                { 37L, "dom004.com", null, null, null, "DUNS004", "N", "Y", "DUDUNS004", null, null, null, null },
                { 38L, "dom004.com", null, null, null, "DUNS005", "Y", "Y", null, null, null, null, null },
                // test larger sales volume with threshold
                { 39L, "dom006.com", null, null, null, "DUNS006", "N", "Y", "DUNS006", null, null, 200000000L, null },
                { 40L, "dom006.com", null, null, null, "DUNS007", "Y", "Y", null, null, null, 199999999L, null },
                { 41L, "dom006.com", null, null, null, "DUNS008", "N", "Y", null, null, null, 200000001L, null },
                // test duns equals duduns
                { 42L, "dom009.com", null, null, null, "DUNS009", "N", "Y", null, null, null, null, null },
                { 43L, "dom009.com", null, null, null, "DUNS010", "N", "Y", "DUNS010", null, null, null, null },
                { 44L, "dom009.com", null, null, null, "DUNS011", "N", "Y", "DUNS010", null, null, null, null },
                // test duns equals guduns
                { 45L, "dom012.com", null, null, null, "DUNS012", "N", "Y", null, null, null, null, null },
                { 46L, "dom012.com", null, null, null, "DUNS013", "N", "Y", null, "DUNS013", null, null, null },
                { 47L, "dom012.com", null, null, null, "DUNS014", "N", "Y", null, "DUNS013", null, null, null },
                // test larger sales volume without threshold
                { 48L, "dom015.com", null, null, null, "DUNS015", "N", "Y", null, null, null, 1000L, null },
                { 49L, "dom015.com", null, null, null, "DUNS016", "Y", "Y", null, null, null, 999L, null },
                // test IsPrimaryLocation
                { 50L, "dom017.com", null, null, null, "DUNS017", "N", "Y", null, null, null, null, null },
                { 51L, "dom017.com", null, null, null, "DUNS018", "Y", "Y", null, null, null, null, null },
                // test USA country
                { 52L, "dom019.com", null, null, "USA", "DUNS019", "N", "Y", null, null, null, null, null },
                { 53L, "dom019.com", null, null, "England", "DUNS020", "N", "Y", null, null, null, null, null },
                // test employee
                { 54L, "dom021.com", null, null, null, "DUNS021", "N", "Y", null, null, 999, null, null },
                { 55L, "dom021.com", null, null, null, "DUNS022", "N", "Y", null, null, null, null, null },
                { 56L, "dom021.com", null, null, null, "DUNS023", "N", "Y", null, null, 1000, null, null },

                // test priority to search by dom+country / dom+country+state / dom+country+zip
                { 100L, "dom100.com", "State100", "Zip100", "Country100", "DUNS100", "Y", "Y", null, "DUNS100", 10000,
                        10000L, "N" },
                { 101L, "dom100.com", "State100", "Zip100", "Country100", "DUNS101", "Y", "Y", null, "DUNS101", 10000,
                        10000L, "Y" },

                // test priority to pick primary domain
                // test IsPrimaryAccount
                { 1000L, "dom1000.com", null, null, null, "DUNS1000", "Y", "Y", null, null, null, null, "N" },
                { 1001L, "dom1001.com", null, null, null, "DUNS1000", "Y", "Y", null, null, null, null, null },
                { 1002L, "dom1002.com", null, null, null, "DUNS1000", "Y", "N", null, null, null, null, "Y" },
                { 1003L, "dom1003.com", null, null, null, "DUNS1000", "Y", "N", null, null, null, null, "N" },
                { 1004L, "dom1004.com", null, null, null, "DUNS1000", "Y", "N", null, null, null, null, null },
                // test IsPrimaryDomain
                { 1005L, "dom1005.com", null, null, null, "DUNS1005", "Y", "N", null, null, null, null, "N" },
                { 1006L, "dom1006.com", null, null, null, "DUNS1005", "Y", "Y", null, null, null, null, "N" },
                { 1007L, "dom1007.com", null, null, null, "DUNS1005", "Y", "N", null, null, null, null, null },
                // test domain only
                { 1008L, "dom1008.com", null, null, null, null, "Y", "N", null, null, null, null, "N" },
        };

        uploadBaseSourceData(ams, baseSourceVersion, columns, data);
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

        uploadBaseSourceData(orbSecDom, baseSourceVersion, columns, data);
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
                { "_DOMAIN_dom32.com_DUNS_NULL", 31L }, //

                { "_DOMAIN_dom001.com_DUNS_NULL", 33L }, //
                { "_DOMAIN_NULL_DUNS_DUNS001", 33L }, //
                { "_DOMAIN_NULL_DUNS_DUNS002", 34L }, //
                { "_DOMAIN_dom001.com_DUNS_DUNS001", 33L }, //
                { "_DOMAIN_dom001.com_DUNS_DUNS002", 34L },//

                { "_DOMAIN_dom003.com_DUNS_NULL", 36L }, //
                { "_DOMAIN_NULL_DUNS_DUNS003", 36L }, //
                { "_DOMAIN_dom003.com_DUNS_DUNS003", 36L },//

                { "_DOMAIN_dom004.com_DUNS_NULL", 37L }, //
                { "_DOMAIN_NULL_DUNS_DUNS004", 37L }, //
                { "_DOMAIN_NULL_DUNS_DUNS005", 38L }, //
                { "_DOMAIN_dom004.com_DUNS_DUNS004", 37L }, //
                { "_DOMAIN_dom004.com_DUNS_DUNS005", 38L }, //

                { "_DOMAIN_dom006.com_DUNS_NULL", 39L }, //
                { "_DOMAIN_NULL_DUNS_DUNS006", 39L }, //
                { "_DOMAIN_NULL_DUNS_DUNS007", 40L }, //
                { "_DOMAIN_NULL_DUNS_DUNS008", 41L }, //
                { "_DOMAIN_dom006.com_DUNS_DUNS006", 39L }, //
                { "_DOMAIN_dom006.com_DUNS_DUNS007", 40L }, //
                { "_DOMAIN_dom006.com_DUNS_DUNS008", 41L }, //

                { "_DOMAIN_dom009.com_DUNS_NULL", 43L }, //
                { "_DOMAIN_NULL_DUNS_DUNS009", 42L }, //
                { "_DOMAIN_NULL_DUNS_DUNS010", 43L }, //
                { "_DOMAIN_NULL_DUNS_DUNS011", 44L }, //
                { "_DOMAIN_dom009.com_DUNS_DUNS009", 42L }, //
                { "_DOMAIN_dom009.com_DUNS_DUNS010", 43L }, //
                { "_DOMAIN_dom009.com_DUNS_DUNS011", 44L }, //

                { "_DOMAIN_dom012.com_DUNS_NULL", 46L }, //
                { "_DOMAIN_NULL_DUNS_DUNS012", 45L }, //
                { "_DOMAIN_NULL_DUNS_DUNS013", 46L }, //
                { "_DOMAIN_NULL_DUNS_DUNS014", 47L }, //
                { "_DOMAIN_dom012.com_DUNS_DUNS012", 45L }, //
                { "_DOMAIN_dom012.com_DUNS_DUNS013", 46L }, //
                { "_DOMAIN_dom012.com_DUNS_DUNS014", 47L }, //

                { "_DOMAIN_dom015.com_DUNS_NULL", 48L }, //
                { "_DOMAIN_NULL_DUNS_DUNS015", 48L }, //
                { "_DOMAIN_NULL_DUNS_DUNS016", 49L }, //
                { "_DOMAIN_dom015.com_DUNS_DUNS015", 48L }, //
                { "_DOMAIN_dom015.com_DUNS_DUNS016", 49L }, //

                { "_DOMAIN_dom017.com_DUNS_NULL", 51L }, //
                { "_DOMAIN_NULL_DUNS_DUNS017", 50L }, //
                { "_DOMAIN_NULL_DUNS_DUNS018", 51L }, //
                { "_DOMAIN_dom017.com_DUNS_DUNS017", 50L }, //
                { "_DOMAIN_dom017.com_DUNS_DUNS018", 51L }, //

                { "_DOMAIN_dom019.com_DUNS_NULL", 52L }, //
                { "_DOMAIN_NULL_DUNS_DUNS019", 52L }, //
                { "_DOMAIN_NULL_DUNS_DUNS020", 53L }, //
                { "_DOMAIN_dom019.com_DUNS_DUNS019", 52L }, //
                { "_DOMAIN_dom019.com_DUNS_DUNS020", 53L }, //
                { "_DOMAIN_dom019.com_DUNS_NULL_COUNTRY_USA_STATE_NULL_ZIPCODE_NULL", 52L }, //
                { "_DOMAIN_dom019.com_DUNS_NULL_COUNTRY_England_STATE_NULL_ZIPCODE_NULL", 53L }, //

                { "_DOMAIN_dom021.com_DUNS_NULL", 56L }, //
                { "_DOMAIN_NULL_DUNS_DUNS021", 54L }, //
                { "_DOMAIN_NULL_DUNS_DUNS022", 55L }, //
                { "_DOMAIN_NULL_DUNS_DUNS023", 56L }, //
                { "_DOMAIN_dom021.com_DUNS_DUNS021", 54L }, //
                { "_DOMAIN_dom021.com_DUNS_DUNS022", 55L }, //
                { "_DOMAIN_dom021.com_DUNS_DUNS023", 56L }, //

                { "_DOMAIN_dom100.com_DUNS_NULL", 101L }, //
                { "_DOMAIN_dom100.com_DUNS_NULL_COUNTRY_Country100_STATE_NULL_ZIPCODE_NULL", 101L }, //
                { "_DOMAIN_dom100.com_DUNS_NULL_COUNTRY_Country100_STATE_State100_ZIPCODE_NULL", 101L }, //
                { "_DOMAIN_dom100.com_DUNS_NULL_COUNTRY_Country100_STATE_NULL_ZIPCODE_Zip100", 101L }, //
                { "_DOMAIN_NULL_DUNS_DUNS100", 100L }, //
                { "_DOMAIN_NULL_DUNS_DUNS101", 101L }, //
                { "_DOMAIN_dom100.com_DUNS_DUNS100", 100L }, //
                { "_DOMAIN_dom100.com_DUNS_DUNS101", 101L }, //

                { "_DOMAIN_dom1000.com_DUNS_DUNS1000", 1000L }, //
                { "_DOMAIN_dom1001.com_DUNS_DUNS1000", 1001L }, //
                { "_DOMAIN_dom1002.com_DUNS_DUNS1000", 1002L }, //
                { "_DOMAIN_dom1003.com_DUNS_DUNS1000", 1003L }, //
                { "_DOMAIN_dom1004.com_DUNS_DUNS1000", 1004L }, //
                { "_DOMAIN_NULL_DUNS_DUNS1000", 1002L }, //
                { "_DOMAIN_dom1000.com_DUNS_NULL", 1000L }, //
                { "_DOMAIN_dom1001.com_DUNS_NULL", 1001L }, //
                { "_DOMAIN_dom1002.com_DUNS_NULL", 1002L }, //
                { "_DOMAIN_dom1003.com_DUNS_NULL", 1003L }, //
                { "_DOMAIN_dom1004.com_DUNS_NULL", 1004L }, //

                { "_DOMAIN_dom1005.com_DUNS_DUNS1005", 1005L }, //
                { "_DOMAIN_dom1006.com_DUNS_DUNS1005", 1006L }, //
                { "_DOMAIN_dom1007.com_DUNS_DUNS1005", 1007L }, //
                { "_DOMAIN_NULL_DUNS_DUNS1005", 1006L }, //
                { "_DOMAIN_dom1005.com_DUNS_NULL", 1005L }, //
                { "_DOMAIN_dom1006.com_DUNS_NULL", 1006L }, //
                { "_DOMAIN_dom1007.com_DUNS_NULL", 1007L }, //

                { "_DOMAIN_dom1008.com_DUNS_NULL", 1008L }
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

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info(String.format("Start to verify intermediate source %s", source));
        try {
            switch (source) {
            case "AccountMasterSeedCleaned":
                verifyAMSeedCleaned(records);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unknown intermediate source %s", source));
            }
        } catch (Exception ex) {
            throw new RuntimeException("Exception in verifyIntermediateResult", ex);
        }
    }

    private void verifyAMSeedCleaned(Iterator<GenericRecord> records) {
        // LatticeID, OperationCodes in LE_OperationLog
        Object[][] expectedData = new Object[][] { //
                { 1L, OperationCode.IS_PRI_LOC }, //
                { 2L, OperationCode.IS_PRI_LOC, OperationCode.IS_PRI_CTRY }, //
                { 3L, OperationCode.IS_PRI_LOC, OperationCode.IS_PRI_CTRY, OperationCode.IS_PRI_ZIP }, //
                { 4L, OperationCode.IS_PRI_LOC, OperationCode.IS_PRI_CTRY, OperationCode.IS_PRI_ST }, //
                { 5L, OperationCode.IS_PRI_LOC, OperationCode.IS_PRI_CTRY, OperationCode.IS_PRI_ZIP,
                        OperationCode.IS_PRI_ST }, //
                { 11L, OperationCode.IS_PRI_LOC }, //
                { 12L, null }, //
                { 21L, OperationCode.IS_PRI_LOC }, //
                { 22L, OperationCode.IS_PRI_LOC }, //
                { 31L, OperationCode.IS_PRI_LOC }, //
                { 33L, OperationCode.IS_PRI_LOC, OperationCode.IS_PRI_DOM }, //
                { 34L, null }, //
                { 35L, null }, //
                { 36L, OperationCode.IS_PRI_LOC }, //
                { 37L, OperationCode.IS_PRI_LOC }, //
                { 38L, null }, //
                { 39L, OperationCode.IS_PRI_LOC }, //
                { 40L, null }, //
                { 41L, null }, //
                { 42L, null }, //
                { 43L, OperationCode.IS_PRI_LOC }, //
                { 44L, null }, //
                { 45L, null }, //
                { 46L, OperationCode.IS_PRI_LOC }, //
                { 47L, null }, //
                { 48L, OperationCode.IS_PRI_LOC }, //
                { 49L, null }, //
                { 50L, null }, //
                { 51L, OperationCode.IS_PRI_LOC }, //
                { 52L, OperationCode.IS_PRI_LOC, OperationCode.IS_PRI_CTRY }, //
                { 53L, OperationCode.IS_PRI_CTRY }, //
                { 54L, null }, //
                { 55L, null }, //
                { 56L, OperationCode.IS_PRI_LOC }, //
                { 100L, null }, //
                { 101L, OperationCode.IS_PRI_LOC, OperationCode.IS_PRI_CTRY, OperationCode.IS_PRI_ZIP,
                        OperationCode.IS_PRI_ST, OperationCode.IS_PRI_DOM }, //
                { 1000L, OperationCode.IS_PRI_LOC, OperationCode.NOT_PRI_DOM }, //
                { 1001L, OperationCode.IS_PRI_LOC, OperationCode.NOT_PRI_DOM }, //
                { 1002L, OperationCode.IS_PRI_LOC, OperationCode.IS_PRI_DOM }, //
                { 1003L, OperationCode.IS_PRI_LOC }, //
                { 1004L, OperationCode.IS_PRI_LOC }, //
                { 1005L, OperationCode.IS_PRI_LOC }, //
                { 1006L, OperationCode.IS_PRI_LOC }, //
                { 1007L, OperationCode.IS_PRI_LOC }, //
                { 1008L, OperationCode.IS_PRI_LOC }, //
        };
        Map<Long, Object[]> expectedOptLogs = new HashMap<>();
        for (Object[] data : expectedData) {
            expectedOptLogs.put((Long) data[0], data);
        }
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Object[] expectedOptLog = expectedOptLogs.get(record.get(DataCloudConstants.LATTIC_ID));
            Assert.assertNotNull(expectedOptLog);
            if (record.get(OperationLogUtils.DEFAULT_FIELD_NAME) == null) {
                Assert.assertNull(expectedOptLog[1]);
            } else {
                String optLog = record.get(OperationLogUtils.DEFAULT_FIELD_NAME).toString();
                IntStream.range(1, expectedOptLog.length) //
                        .parallel() //
                        .mapToObj(i -> (OperationCode) expectedOptLog[i]) //
                        .forEach(code -> Assert.assertTrue(optLog.contains(code.name())));
            }

        }
    }

}