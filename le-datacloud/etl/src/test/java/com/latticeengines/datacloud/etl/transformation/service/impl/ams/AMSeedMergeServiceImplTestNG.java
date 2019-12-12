package com.latticeengines.datacloud.etl.transformation.service.impl.ams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.ams.AMSeedMerge;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class AMSeedMergeServiceImplTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AMSeedMergeServiceImplTestNG.class);

    private GeneralSource amsMerged = new GeneralSource("AccountMasterSeedMerged");
    private GeneralSource amsStd = new GeneralSource("AccountMasterSeedStandard");
    private GeneralSource dnbCacheSeed = new GeneralSource("DnBCacheSeed");
    private GeneralSource leCacheSeed = new GeneralSource("LatticeCacheSeed");

    private static final String DUNS = "DUNS";
    private static final String DUNS_NUMBER = "DUNS_NUMBER";
    private static final String LE_PRIMARY_DUNS = "LE_PRIMARY_DUNS";
    private static final String GLOBAL_ULTIMATE_DUNS_NUMBER = "GLOBAL_ULTIMATE_DUNS_NUMBER";
    private static final String HEADQUARTER_PARENT_DUNS_NUMBER = "HEADQUARTER_PARENT_DUNS_NUMBER";
    private static final String DOMAIN = "Domain";
    private static final String LE_DOMAIN = "LE_DOMAIN";
    private static final String NAME = "Name";
    private static final String BUSINESS_NAME = "BUSINESS_NAME";
    private static final String STREET = "Street";
    private static final String STREET_ADDRESS = "STREET_ADDRESS";
    private static final String ZIPCODE = "ZipCode";
    private static final String POSTAL_CODE = "POSTAL_CODE";
    private static final String CITY = "City";
    private static final String CITY_NAME = "CITY_NAME";
    private static final String STATE = "State";
    private static final String STATE_PROVINCE_NAME = "STATE_PROVINCE_NAME";
    private static final String COUNTRY = "Country";
    private static final String COUNTRY_NAME = "COUNTRY_NAME";
    private static final String LE_COUNTRY = "LE_COUNTRY";
    private static final String PHONE_NUMBER = "PhoneNumber";
    private static final String LE_COMPANY_PHONE = "LE_COMPANY_PHONE";
    private static final String REVENUE_RANGE = "RevenueRange";
    private static final String LE_REVENUE_RANGE = "LE_REVENUE_RANGE";
    private static final String EMPLOYEE_RANGE = "EmployeeRange";
    private static final String LE_EMPLOYEE_RANGE = "LE_EMPLOYEE_RANGE";
    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";
    private static final String LE_IS_PRIMARY_LOCATION = "LE_IS_PRIMARY_LOCATION";
    private static final String LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    private static final String EMPLOYEES_HERE = "EMPLOYEES_HERE";
    private static final String SALES_VOLUME_US_DOLLARS = "SALES_VOLUME_US_DOLLARS";
    private static final String PRIMARY_INDUSTRY = "PrimaryIndustry";
    private static final String LE_PRIMARY_INDUSTRY = "LE_PRIMARY_INDUSTRY";
    private static final String LE_INDUSTRY = "LE_INDUSTRY";
    private static final String SOURCE = "__Source__";
    private static final String DOMAIN_SOURCE = "DomainSource";
    private static final String OUT_OF_BUSINESS_INDICATOR = "OUT_OF_BUSINESS_INDICATOR";
    private static final String EMPLOYEES_TOTAL = "EMPLOYEES_TOTAL";
    private static final String CHIEF_EXECUTIVE_OFFICER_NAME = "CHIEF_EXECUTIVE_OFFICER_NAME";
    private static final String SOURCE_PRIORITY = "__Source_Priority__";

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        prepareDnBSeed();
        prepareLeSeed();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return amsStd.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

        configuration.setName("AccountMasterSeedMerge");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(dnbCacheSeed.getSourceName());
        baseSources.add(leCacheSeed.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(AMSeedMerge.TRANSFORMER_NAME);
        step1.setTargetSource(amsMerged.getSourceName());
        String confParamStr1 = getTransformerConfig();
        step1.setConfiguration(confParamStr1);

        TransformationStepConfig step2 = new TransformationStepConfig();
        List<Integer> inputSteps = new ArrayList<>();
        inputSteps.add(0);
        step2.setInputSteps(inputSteps);
        step2.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step2.setTargetSource(amsStd.getSourceName());
        String confParamStr2 = getStandardizationTransformerConfig();
        step2.setConfiguration(confParamStr2);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);
        steps.add(step2);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private String getStandardizationTransformerConfig() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] countryFields = { "Country", "LE_COUNTRY" };
        conf.setCountryFields(countryFields);
        String[] stateFields = { "State" };
        conf.setStateFields(stateFields);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { //
                StandardizationStrategy.COUNTRY, StandardizationStrategy.STATE };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    private String getTransformerConfig() {
        TransformerConfig conf = new TransformerConfig();
        conf.setTransformer(AMSeedMerge.TRANSFORMER_NAME);
        return JsonUtils.serialize(conf);
    }

    private void prepareDnBSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(LE_DOMAIN, String.class));
        columns.add(Pair.of(LE_IS_PRIMARY_LOCATION, String.class));
        columns.add(Pair.of(LE_IS_PRIMARY_DOMAIN, String.class));
        columns.add(Pair.of(LE_NUMBER_OF_LOCATIONS, Integer.class));
        columns.add(Pair.of(DUNS_NUMBER, String.class));
        columns.add(Pair.of(LE_PRIMARY_DUNS, String.class));
        columns.add(Pair.of(BUSINESS_NAME, String.class));
        columns.add(Pair.of(STATE_PROVINCE_NAME, String.class));
        columns.add(Pair.of(COUNTRY_NAME, String.class));
        columns.add(Pair.of(LE_COUNTRY, String.class));
        columns.add(Pair.of(SALES_VOLUME_US_DOLLARS, Long.class));
        columns.add(Pair.of(EMPLOYEES_HERE, Integer.class));
        columns.add(Pair.of(LE_PRIMARY_INDUSTRY, String.class));
        columns.add(Pair.of(STREET_ADDRESS, String.class));
        columns.add(Pair.of(CITY_NAME, String.class));
        columns.add(Pair.of(POSTAL_CODE, String.class));
        columns.add(Pair.of(LE_COMPANY_PHONE, String.class));
        columns.add(Pair.of(LE_REVENUE_RANGE, String.class));
        columns.add(Pair.of(LE_EMPLOYEE_RANGE, String.class));
        columns.add(Pair.of(GLOBAL_ULTIMATE_DUNS_NUMBER, String.class));
        columns.add(Pair.of(LE_INDUSTRY, String.class));
        columns.add(Pair.of(OUT_OF_BUSINESS_INDICATOR, String.class));
        columns.add(Pair.of(EMPLOYEES_TOTAL, String.class));
        columns.add(Pair.of(CHIEF_EXECUTIVE_OFFICER_NAME, String.class));
        columns.add(Pair.of(HEADQUARTER_PARENT_DUNS_NUMBER, String.class));
        uploadBaseSourceData(dnbCacheSeed.getSourceName(), baseSourceVersion, columns, dnbData);
    }

    private Object[][] dnbData = new Object[][] { //
            { "a.com", "Y", "Y", 2, "111", "111", "DnBName111", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI111", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { null, "Y", "N", 3, "222", "222", "DnBName222", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI222", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "c.com", "Y", "Y", 4, "333", "333", "DnBName333", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI333", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "e.com", "Y", "Y", 5, "444", "444", "DnBName444", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI444", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "e.com", "N", "Y", 6, "555", "444", "DnBName555", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI555", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { null, "Y", "Y", 7, "666", "666", "DnBName666", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI666", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { null, "N", "N", 8, "777", "666", "DnBName777", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI777", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { null, "N", "N", 9, "888", "999", "DnBName888", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI888", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "g.com", "Y", "Y", 10, "999", "999", "DnBName999", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI999", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "g.com", "N", "Y", 11, "101010", "999", "DnBName101010", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI101010", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { null, "N", "N", 12, "111111", "121212", "DnBName111111", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPI111111", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "h.com", "Y", "Y", 13, "121212", "121212", "DnBName121212", "CA", "United States", "England", 1000000L,
                    1000, "DnBPI121212", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "h.com", "N", "Y", 14, "131313", "121212", "DnBName131313", "CA", "United States", "England", 1000000L,
                    1000, "DnBPI131313", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { null, "Y", "N", 15, "NoDu111", null, "DnBNameNoDu111", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPINoDu111", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "b.com", "Y", "Y", 16, "NoDu222", null, "DnBNameNoDu222", "CA", "United States", "England", 1000000L,
                    1000, "DnBPINoDu222", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "c.com", "Y", "Y", 16, "NoDu222", null, "DnBNameNoDu222", "CA", "United States", "England", 1000000L,
                    1000, "DnBPINoDu222", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "b.com", "Y", "Y", 17, "NoDu333", null, "DnBNameNoDu333", "CA", "United States", "England", 1000000L,
                    1000, "DnBPINoDu333", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "c.com", "Y", "N", 17, "NoDu444", null, "DnBNameNoDu444", "CA", "United States", "England", 1000000L,
                    1000, "DnBPINoDu444", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { null, "Y", "N", 17, "NoDu555", null, "DnBNameNoDu555", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPINoDu555", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { null, "Y", "Y", 17, "NoDu666", null, "DnBNameNoDu666", "CA", "United States", "England", 1000000L, 1000,
                    "DnBPINoDu666", null, null, null, null, null, null, null, null, null, null, null, null }, //
            { "z.com", "Y", "N", 18, "DunsTestIsPriDom111", null, "DnBNameTestIsPriDom111", "CA", "United States",
                    "England", 1000000L, 1000, "DnBPITestIsPriDom111", null, null, null, null, null, null, null, null,
                    null, null, null, null }, //
            { null, "Y", "Y", 19, "DunsTestIsPriDom222", null, "DnBNameTestIsPriDom222", "CA", "United States",
                    "England", 1000000L, 1000, "DnBPITestIsPriDom222", null, null, null, null, null, null, null, null,
                    null, null, null, null }, //
            { "x.com", "Y", "Y", 20, "DunsTestIsPriDom333", null, "DnBNameTestIsPriDom333", "CA", "United States",
                    "England", 1000000L, 1000, "DnBPITestIsPriDom333", null, null, null, null, null, null, null, null,
                    null, null, null, null }, //
            { "y.com", "Y", "N", 20, "DunsTestIsPriDom333", null, "DnBNameTestIsPriDom333", "CA", "United States",
                    "England", 1000000L, 1000, "DnBPITestIsPriDom333", null, null, null, null, null, null, null, null,
                    null, null, null, null }, //
            { "zz.com", "Y", "Y", 21, "DunsTestPartialMissDom", null, "DnBNameTestPartialMissDom", "CA",
                    "United States", "England", 1000000L, 1000, "DnBPITestPartialMissDom", null, null, null, null, null,
                    null, null, null, null, null, null, null }, //
            { "yy.com", "Y", "Y", 21, "DunsTestPartialMissDom", null, "DnBNameTestPartialMissDom", "CA",
                    "United States", "England", 1000000L, 1000, "DnBPITestPartialMissDom", null, null, null, null, null,
                    null, null, null, null, null, null, null }, //
            { null, "Y", "Y", 21, "DunsTestPartialMissDom", null, "DnBNameTestPartialMissDom", "CA", "United States",
                    "England", 1000000L, 1000, "DnBPITestPartialMissDom", null, null, null, null, null, null, null,
                    null, null, null, null, null }, //
    };

    private void prepareLeSeed() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(DOMAIN, String.class));
        columns.add(Pair.of(NAME, String.class));
        columns.add(Pair.of(CITY, String.class));
        columns.add(Pair.of(STATE, String.class));
        columns.add(Pair.of(COUNTRY, String.class));
        columns.add(Pair.of(DUNS, String.class));
        columns.add(Pair.of(STREET, String.class));
        columns.add(Pair.of(ZIPCODE, String.class));
        columns.add(Pair.of(PHONE_NUMBER, String.class));
        columns.add(Pair.of(REVENUE_RANGE, String.class));
        columns.add(Pair.of(EMPLOYEE_RANGE, String.class));
        columns.add(Pair.of(PRIMARY_INDUSTRY, String.class));
        columns.add(Pair.of(SOURCE, String.class));
        columns.add(Pair.of(SOURCE_PRIORITY, Integer.class));
        uploadBaseSourceData(leCacheSeed.getSourceName(), baseSourceVersion, columns, leData);
    }

    private Object[][] leData = new Object[][] { //
            { "a.com", "LeName111", null, null, null, "111", null, null, null, null, null, "LePI111", "Orb", 1 }, //
            { "b.com", "LeName222", null, null, null, "222", null, null, null, null, null, "LePI222", "HG", 1 }, //
            { "d.com", "LeName333", null, null, null, "333", null, null, null, null, null, "LePI333", "RTS", 1 }, //
            { "e.com", "LeName444", null, null, null, "444", null, null, null, null, null, "LePI444", "Orb", 1 }, //
            { "e.com", "LeName555", null, null, null, "555", null, null, null, null, null, "LePI555", "HG", 1 }, //
            { "f.com", "LeName666", null, null, null, "666", null, null, null, null, null, "LePI666", "RTS", 1 }, //
            { "f.com", "LeName777", null, null, null, "777", null, null, null, null, null, "LePI777", "Orb", 1 }, //
            { "g.com", "LeName888", null, null, null, "888", null, null, null, null, null, "LePI888", "HG", 1 }, //
            { "g.com", "LeName999", null, null, null, "999", null, null, null, null, null, "LePI999", "RTS", 1 }, //
            { "i.com", "LeName111111", null, null, null, "111111", null, null, null, null, null, "LePI111111", "Orb",
                    1 }, //
            { "ii.com", "LeName121212", null, null, null, "121212", null, null, null, null, null, "LePI121212", "HG",
                    1 }, //
            { "a.com", "LeNameNoDu111", null, null, null, "NoDu111", null, null, null, null, null, "LePINoDu111", "RTS",
                    1 }, //
            { "c.com", "LeNameNoDu2221", null, null, null, "NoDu222", null, null, null, null, null, "LePINoDu2221",
                    "Orb", 1 }, //
            { "d.com", "LeNameNoDu2222", null, null, null, "NoDu222", null, null, null, null, null, "LePINoDu2222",
                    "HG", 1 }, //
            { "j.com", "LeNamej.com", null, null, null, null, null, null, null, null, null, "LePIj", "RTS", 1 }, //
            { "k.com", "LeNamek.com", null, null, null, null, null, null, null, null, null, "LePIk", "Orb", 1 }, //
            { "l.com", "LeNamel.com", null, null, null, "InvalidLeDuns", null, null, null, null, null, "LePIl", "HG",
                    1 }, //
            { "l.com", "LeNamel.com", null, null, null, null, null, null, null, null, null, "LePIl", "HG",
                        0 }, // Test dedup and sort by source_priority
            { "g.com", "LeNameg.com", null, null, null, "InvalidLeDuns", null, null, null, null, null, "LePIg", "RTS",
                    1 }, //
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        // Schema: DOMAIN, DUNS, LE_PRIMARY_DUNS, LE_IS_PRIMARY_DOMAIN,
        // LE_IS_PRIMARY_LOCATION, LE_NUMBER_OF_LOCATIONS, NAME,
        // PRIMARY_INDUSTRY, DOMAIN_SOURCE, EMPLOYEES_HERE,
        // SALES_VOLUME_US_DOLLARS, COUNTRY, LE_COUNTRY, STATE
        Object[][] expectedResults = new Object[][] {
                { "a.com", "111", "111", "Y", "Y", 2, "DnBName111", "DnBPI111", "Orb", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "b.com", "222", "222", "Y", "Y", 3, "DnBName222", "DnBPI222", "HG", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "d.com", "333", "333", "Y", "Y", 4, "DnBName333", "DnBPI333", "RTS", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "c.com", "333", "333", "N", "Y", 4, "DnBName333", "DnBPI333", "DnB", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "e.com", "444", "444", "Y", "Y", 5, "DnBName444", "DnBPI444", "Orb", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "e.com", "555", "444", "Y", "N", 6, "DnBName555", "DnBPI555", "HG", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "f.com", "666", "666", "Y", "Y", 7, "DnBName666", "DnBPI666", "RTS", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "f.com", "777", "666", "Y", "N", 8, "DnBName777", "DnBPI777", "Orb", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "g.com", "888", "999", "Y", "N", 9, "DnBName888", "DnBPI888", "HG", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "g.com", "999", "999", "Y", "Y", 10, "DnBName999", "DnBPI999", "RTS", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "g.com", "101010", "999", "Y", "N", 11, "DnBName101010", "DnBPI101010", "DnB", 1000, 1000000L, "USA",
                        "UNITED KINGDOM", "CALIFORNIA" },
                { "i.com", "111111", "121212", "Y", "N", 12, "DnBName111111", "DnBPI111111", "Orb", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "ii.com", "111111", "121212", "Y", "N", 12, "DnBName111111", "DnBPI111111", "HG", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "h.com", "121212", "121212", "N", "Y", 13, "DnBName121212", "DnBPI121212", "DnB", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "i.com", "121212", "121212", "Y", "Y", 13, "DnBName121212", "DnBPI121212", "Orb", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "ii.com", "121212", "121212", "Y", "Y", 13, "DnBName121212", "DnBPI121212", "HG", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "h.com", "131313", "121212", "Y", "N", 14, "DnBName131313", "DnBPI131313", "DnB", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "a.com", "NoDu111", null, "Y", "Y", 15, "DnBNameNoDu111", "DnBPINoDu111", "RTS", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "b.com", "NoDu222", null, "N", "Y", 16, "DnBNameNoDu222", "DnBPINoDu222", "DnB", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "c.com", "NoDu222", null, "Y", "Y", 16, "DnBNameNoDu222", "DnBPINoDu222", "Orb", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "d.com", "NoDu222", null, "Y", "Y", 16, "DnBNameNoDu222", "DnBPINoDu222", "HG", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "b.com", "NoDu333", null, "Y", "Y", 17, "DnBNameNoDu333", "DnBPINoDu333", "DnB", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "c.com", "NoDu444", null, "Y", "Y", 17, "DnBNameNoDu444", "DnBPINoDu444", "DnB", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { null, "NoDu555", null, "N", "Y", 17, "DnBNameNoDu555", "DnBPINoDu555", "DnB", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { null, "NoDu666", null, "N", "Y", 17, "DnBNameNoDu666", "DnBPINoDu666", "DnB", 1000, 1000000L,
                        "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "j.com", null, null, "Y", "Y", 1, "LeNamej.com", "LePIj", "RTS", null, null, null, null, null },
                { "k.com", null, null, "Y", "Y", 1, "LeNamek.com", "LePIk", "Orb", null, null, null, null, null },
                { "l.com", null, null, "Y", "Y", 1, "LeNamel.com", "LePIl", "HG", null, null, null, null, null },
                { "z.com", "DunsTestIsPriDom111", null, "Y", "Y", 18, "DnBNameTestIsPriDom111",
                        "DnBPITestIsPriDom111", "DnB", 1000, 1000000L, "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { null, "DunsTestIsPriDom222", null, "N", "Y", 19, "DnBNameTestIsPriDom222", "DnBPITestIsPriDom222",
                        "DnB", 1000, 1000000L, "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "x.com", "DunsTestIsPriDom333", null, "Y", "Y", 20, "DnBNameTestIsPriDom333",
                        "DnBPITestIsPriDom333", "DnB", 1000, 1000000L, "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "y.com", "DunsTestIsPriDom333", null, "N", "Y", 20, "DnBNameTestIsPriDom333",
                        "DnBPITestIsPriDom333", "DnB", 1000, 1000000L, "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "zz.com", "DunsTestPartialMissDom", null, "Y", "Y", 21, "DnBNameTestPartialMissDom",
                        "DnBPITestPartialMissDom", "DnB", 1000, 1000000L, "USA", "UNITED KINGDOM", "CALIFORNIA" },
                { "yy.com", "DunsTestPartialMissDom", null, "Y", "Y", 21, "DnBNameTestPartialMissDom",
                        "DnBPITestPartialMissDom", "DnB", 1000, 1000000L, "USA", "UNITED KINGDOM", "CALIFORNIA" },

        };
        Map<String, Object[]> expectedMap = Arrays.stream(expectedResults)
                .collect(Collectors.toMap(x -> String.valueOf(x[0]) + "_" + String.valueOf(x[1]), x -> x));
        int rowNum = 0;
        Set<String> domainDuns = new HashSet<>();
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            String domain = String.valueOf(record.get(DOMAIN));
            String duns = String.valueOf(record.get(DUNS));
            String key = domain + "_" + duns;
            Assert.assertTrue(expectedMap.containsKey(key));
            domainDuns.add(key);
            Object[] expectedObject = expectedMap.get(key);
            Assert.assertTrue(isObjEquals(record.get(LE_PRIMARY_DUNS), expectedObject[2]));
            Assert.assertTrue(isObjEquals(record.get(LE_IS_PRIMARY_DOMAIN), expectedObject[3]));
            Assert.assertTrue(isObjEquals(record.get(LE_IS_PRIMARY_LOCATION), expectedObject[4]));
            Assert.assertTrue(isObjEquals(record.get(LE_NUMBER_OF_LOCATIONS), expectedObject[5]));
            Assert.assertTrue(isObjEquals(record.get(NAME), expectedObject[6]));
            Assert.assertTrue(isObjEquals(record.get(PRIMARY_INDUSTRY), expectedObject[7]));
            Assert.assertTrue(isObjEquals(record.get(DOMAIN_SOURCE), expectedObject[8]));
            Assert.assertTrue(isObjEquals(record.get(EMPLOYEES_HERE), expectedObject[9]));
            Assert.assertTrue(isObjEquals(record.get(SALES_VOLUME_US_DOLLARS), expectedObject[10]));
            Assert.assertTrue(isObjEquals(record.get(COUNTRY), expectedObject[11]));
            Assert.assertTrue(isObjEquals(record.get(LE_COUNTRY), expectedObject[12]));
            Assert.assertTrue(isObjEquals(record.get(STATE), expectedObject[13]));
            rowNum++;
        }
        Assert.assertEquals(rowNum, expectedResults.length);
        Assert.assertEquals(domainDuns.size(), expectedResults.length);
    }

}
