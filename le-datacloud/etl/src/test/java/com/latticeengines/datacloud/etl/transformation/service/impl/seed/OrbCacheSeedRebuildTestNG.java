package com.latticeengines.datacloud.etl.transformation.service.impl.seed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.seed.OrbCacheSeedRebuildFlow;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateIndustryStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateRangeStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.FieldType;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.OrbCacheSeedRebuildConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class OrbCacheSeedRebuildTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(OrbCacheSeedRebuildTestNG.class);

    private GeneralSource source = new GeneralSource("OrbCacheSeedStandard");
    private GeneralSource orbCompanyRaw = new GeneralSource("OrbCompanyRaw");
    private GeneralSource orbDomainRaw = new GeneralSource("OrbDomainRaw");

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(orbCompanyRaw, "OrbCompanyRaw", baseSourceVersion);
        uploadBaseSourceFile(orbDomainRaw, "OrbDomainRaw", baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmIntermediateResultFile();
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

        configuration.setName("OrbCacheSeedRebuild");
        configuration.setVersion(targetVersion);

        // Field standardization for OrbCompany
        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add(orbCompanyRaw.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step1.setTargetSource("OrbCompanyRawMarked");
        String confParamStr1 = getStandardizationTransformerConfigForOrbCompanyMarker();
        step1.setConfiguration(confParamStr1);

        // Data cleanup for OrbCompany
        TransformationStepConfig step2 = new TransformationStepConfig();
        baseSources = new ArrayList<String>();
        baseSources.add("OrbCompanyRawMarked");
        step2.setBaseSources(baseSources);
        step2.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step2.setTargetSource("OrbCompany");
        String confParamStr2 = getStandardizationTransformerConfigForCleanup();
        step2.setConfiguration(confParamStr2);

        // Field standardization for OrbDomain
        TransformationStepConfig step3 = new TransformationStepConfig();
        baseSources = new ArrayList<String>();
        baseSources.add(orbDomainRaw.getSourceName());
        step3.setBaseSources(baseSources);
        step3.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step3.setTargetSource("OrbDomainRawMarked");
        String confParamStr3 = getStandardizationTransformerConfigForOrbDomain();
        step3.setConfiguration(confParamStr3);

        // Data cleanup for OrbDomain
        TransformationStepConfig step4 = new TransformationStepConfig();
        baseSources = new ArrayList<String>();
        baseSources.add("OrbDomainRawMarked");
        step4.setBaseSources(baseSources);
        step4.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step4.setTargetSource("OrbDomain");
        String confParamStr4 = getStandardizationTransformerConfigForCleanup();
        step4.setConfiguration(confParamStr4);

        // Generate OrbCacheSeed
        TransformationStepConfig step5 = new TransformationStepConfig();
        baseSources = new ArrayList<String>();
        baseSources.add("OrbCompany");
        baseSources.add("OrbDomain");
        step5.setBaseSources(baseSources);
        step5.setTransformer(OrbCacheSeedRebuildFlow.TRANSFORMER);
        step5.setTargetSource("OrbCacheSeed");
        String confParamStr5 = getOrbCacheSeedRebuildConfig();
        step5.setConfiguration(confParamStr5);

        // Generate OrbCacheSeedStantard
        TransformationStepConfig step6 = new TransformationStepConfig();
        baseSources = new ArrayList<String>();
        baseSources.add("OrbCacheSeed");
        step6.setBaseSources(baseSources);
        step6.setTransformer(DataCloudConstants.TRANSFORMER_STANDARDIZATION);
        step6.setTargetSource(source.getSourceName());
        String confParamStr6 = getOrbCacheSeedStandardConfig();
        step6.setConfiguration(confParamStr6);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);
        steps.add(step4);
        steps.add(step5);
        steps.add(step6);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private String getStandardizationTransformerConfigForOrbCompanyMarker() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] domainFields = { "Website" };
        conf.setDomainFields(domainFields);
        String[] convertTypeFields = {"Employee", "LocationEmployee","FacebookLikes", "TwitterFollowers", "TotalAmountRaised", "LastFundingRoundAmount", "SearchRank" };
        conf.setConvertTypeFields(convertTypeFields);
        TypeConvertStrategy[] convertTypeStrategies = { TypeConvertStrategy.STRING_TO_INT,
                TypeConvertStrategy.STRING_TO_INT, TypeConvertStrategy.STRING_TO_LONG,
                TypeConvertStrategy.STRING_TO_LONG, TypeConvertStrategy.STRING_TO_LONG,
                TypeConvertStrategy.STRING_TO_LONG, TypeConvertStrategy.STRING_TO_LONG };
        conf.setConvertTypeStrategies(convertTypeStrategies);
        String[] dedupFields = { "OrbNum" };
        conf.setDedupFields(dedupFields);
        String[] addConsolidatedRangeFields = { "ConsolidateEmployeeRange", "ConsolidateRevenueRange" };
        conf.setAddConsolidatedRangeFields(addConsolidatedRangeFields);
        ConsolidateRangeStrategy[] strategies = { ConsolidateRangeStrategy.MAP_VALUE,
                ConsolidateRangeStrategy.MAP_RANGE };
        conf.setConsolidateRangeStrategies(strategies);
        String[] rangeInputFields = { "Employee", "RevenueRange" };
        conf.setRangeInputFields(rangeInputFields);
        String[] rangeMapFileNames = { "EmployeeRangeMapping.txt", "OrbRevenueRangeMapping.txt" };
        conf.setRangeMapFileNames(rangeMapFileNames);
        conf.setConsolidateIndustryStrategy(ConsolidateIndustryStrategy.MAP_INDUSTRY);
        conf.setAddConsolidatedIndustryField("PrimaryIndustry");
        conf.setIndustryField("Industry");
        conf.setIndustryMapFileName("OrbIndustryMapping.txt");
        String markerExpression = "OrbNum != null && Website != null";
        conf.setMarkerExpression(markerExpression);
        String[] markerCheckFields = { "OrbNum", "Website" };
        conf.setMarkerCheckFields(markerCheckFields);
        String markerField = "IsValid";
        conf.setMarkerField(markerField);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { StandardizationStrategy.DEDUP,
                StandardizationStrategy.DOMAIN, StandardizationStrategy.CONVERT_TYPE,
                StandardizationStrategy.CONSOLIDATE_RANGE,
                StandardizationStrategy.CONSOLIDATE_INDUSTRY, StandardizationStrategy.MARKER };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    private String getStandardizationTransformerConfigForCleanup() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String filterExpression = "IsValid == true";
        conf.setFilterExpression(filterExpression);
        String[] filterFields = { "IsValid" };
        conf.setFilterFields(filterFields);
        String[] discardFields = { "IsValid" };
        conf.setDiscardFields(discardFields);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { StandardizationStrategy.FILTER,
                StandardizationStrategy.DISCARD };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    private String getStandardizationTransformerConfigForOrbDomain() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String[] dedupFields = { "OrbNum", "WebDomain" };
        conf.setDedupFields(dedupFields);
        String[] domainFields = { "WebDomain" };
        conf.setDomainFields(domainFields);
        String[] convertTypeFields = { "DomainHasEmail", "DomainHasWebsite", "DomainIsEmailHosting" };
        conf.setConvertTypeFields(convertTypeFields);
        TypeConvertStrategy[] convertTypeStrategies = { TypeConvertStrategy.STRING_TO_BOOLEAN,
                TypeConvertStrategy.STRING_TO_BOOLEAN, TypeConvertStrategy.STRING_TO_BOOLEAN };
        conf.setConvertTypeStrategies(convertTypeStrategies);
        String markerExpression = "OrbNum != null && WebDomain != null";
        conf.setMarkerExpression(markerExpression);
        String[] markerCheckFields = { "OrbNum", "WebDomain" };
        conf.setMarkerCheckFields(markerCheckFields);
        String markerField = "IsValid";
        conf.setMarkerField(markerField);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { StandardizationStrategy.DOMAIN,
                StandardizationStrategy.DEDUP, StandardizationStrategy.CONVERT_TYPE, StandardizationStrategy.MARKER };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    private String getOrbCacheSeedRebuildConfig() {
        OrbCacheSeedRebuildConfig conf = new OrbCacheSeedRebuildConfig();
        conf.setCompanyFileOrbNumField("OrbNum");
        conf.setCompanyFileEntityTypeField("EntityType");
        conf.setCompanyFileDomainField("Website");
        conf.setCompanyFileWebDomainsField("WebDomain");
        conf.setDomainFileOrbNumField("OrbNum");
        conf.setDomainFileDomainField("WebDomain");
        conf.setDomainFileHasEmailField("DomainHasEmail");
        conf.setOrbCacheSeedDomainField("Domain");
        conf.setOrbCacheSeedPrimaryDomainField("PrimaryDomain");
        conf.setOrbCacheSeedIsSecondaryDomainField("IsSecondaryDomain");
        conf.setOrbCacheSeedDomainHasEmailField("DomainHasEmail");
        return JsonUtils.serialize(conf);
    }

    private String getOrbCacheSeedStandardConfig() {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String filterExpression = "IsSecondaryDomain == false && (DomainHasEmail == null || DomainHasEmail == false)";
        conf.setFilterExpression(filterExpression);
        String[] filterFields = { "IsSecondaryDomain", "DomainHasEmail" };
        conf.setFilterFields(filterFields);
        String[] retainFields = { "OrbNum", "Domain", "Name", "Country", "State", "City", "Address1", "Zip", "Phone",
                "ConsolidateRevenueRange", "ConsolidateEmployeeRange", "PrimaryIndustry" };
        conf.setRetainFields(retainFields);
        String[][] renameFields = { { "OrbNum", "ID" }, { "Address1", "Street" }, { "Zip", "ZipCode" },
                { "Phone", "PhoneNumber" }, { "ConsolidateRevenueRange", "RevenueRange" },
                { "ConsolidateEmployeeRange", "EmployeeRange" } };
        conf.setRenameFields(renameFields);

        String[] addFields = { "DUNS" };
        conf.setAddFields(addFields);
        Object[] addFieldValues = { null };
        conf.setAddFieldValues(addFieldValues);
        StandardizationTransformerConfig.FieldType[] addFieldTypes = { FieldType.STRING };
        conf.setAddFieldTypes(addFieldTypes);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { StandardizationStrategy.FILTER,
                StandardizationStrategy.RETAIN, StandardizationStrategy.RENAME, StandardizationStrategy.ADD_FIELD };
        conf.setSequence(sequence);
        return JsonUtils.serialize(conf);
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Object[][] expectedData = { { "1", "google.com", "Company1", "101-250M", ">10,000", "Media" },
                { "3", "yahoo.com", "Company3", "null", "201-500", "null" },
                { "4", "baidu.com", "Company4", "null", "null", "null" } };
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String id = String.valueOf(record.get("ID"));
            String domain = String.valueOf(record.get("Domain"));
            String name = String.valueOf(record.get("Name"));
            String revenueRange = String.valueOf(record.get("RevenueRange"));
            String employeeRange = String.valueOf(record.get("EmployeeRange"));
            String primaryIndustry = String.valueOf(record.get("PrimaryIndustry"));
            boolean flag = false;
            for (Object[] data : expectedData) {
                if (id.equals(data[0]) && domain.equals(data[1]) && name.equals(data[2]) && revenueRange.equals(data[3])
                        && employeeRange.equals(data[4]) && primaryIndustry.equals(data[5])) {
                    flag = true;
                    break;
                }
            }
            Assert.assertTrue(flag);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 3);
    }

    void confirmIntermediateResultFile() {
        String path = hdfsPathBuilder.constructSnapshotDir("OrbCacheSeed", targetVersion).toString();
        log.info("Checking for result file: " + path);
        List<String> files;
        try {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(files.size() >= 2);
        for (String file : files) {
            if (!file.endsWith(SUCCESS_FLAG)) {
                Assert.assertTrue(file.endsWith(".avro"));
                continue;
            }
            Assert.assertTrue(file.endsWith(SUCCESS_FLAG));
        }

        Iterator<GenericRecord> records = AvroUtils.iterateAvroFiles(yarnConfiguration, path + "/*.avro");
        verifyIntermediateResultAvroRecords(records);
    }

    protected void verifyIntermediateResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Object[][] expectedData = {
                { "1", "Company1", "company", 50000, 50000, 50000L, 50000L, 50000L, 50000L, 50000L, ">10,000",
                        "101-250M", "Media", "googlecompany3.com", "google.com", true, null },
                { "1", "Company1", "company", 50000, 50000, 50000L, 50000L, 50000L, 50000L, 50000L, ">10,000",
                        "101-250M", "Media", "google.com", "google.com", false, null },
                { "1", "Company1", "company", 50000, 50000, 50000L, 50000L, 50000L, 50000L, 50000L, ">10,000",
                        "101-250M", "Media", "googlecompany2.com", "google.com", true, null },
                { "1", "Company1", "company", 50000, 50000, 50000L, 50000L, 50000L, 50000L, 50000L, ">10,000",
                        "101-250M", "Media", "googledomain1.com", "google.com", true, true },
                { "1", "Company1", "company", 50000, 50000, 50000L, 50000L, 50000L, 50000L, 50000L, ">10,000",
                        "101-250M", "Media", "googlecompany1.com", "google.com", true, null },
                { "1", "Company1", "company", 50000, 50000, 50000L, 50000L, 50000L, 50000L, 50000L, ">10,000",
                        "101-250M", "Media", "googledomain2.com", "google.com", true, false },
                { "2", "Company2", "company", null, null, null, null, null, null, null, "null", "null", "null",
                        "google.com", "google.com", false, true },
                { "2", "Company2", "company", null, null, null, null, null, null, null, "null", "null", "null",
                        "googledomain1.com", "google.com", true, false },
                { "3", "Company3", "company", 500, 500, 500L, 500L, 500L, 500L, 500L, "201-500", "null", "null",
                        "yahoocompany1.com", "yahoo.com", true, null },
                { "3", "Company3", "company", 500, 500, 500L, 500L, 500L, 500L, 500L, "201-500", "null", "null",
                        "yahoocompany2.com", "yahoo.com", true, null },
                { "3", "Company3", "company", 500, 500, 500L, 500L, 500L, 500L, 500L, "201-500", "null", "null",
                        "yahoo.com", "yahoo.com", false, null },
                { "4", "Company4", "company", null, null, null, null, null, null, null, "null", "null", "null",
                        "baidu.com", "baidu.com", false, null } };
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String orbNum = String.valueOf(record.get("OrbNum"));
            String name = String.valueOf(record.get("Name"));
            String entityType = String.valueOf(record.get("EntityType"));
            Integer employee = (record.get("Employee") == null) ? null : ((Integer) record.get("Employee"));
            Integer locationEmployee = (record.get("Employee") == null) ? null
                    : ((Integer) record.get("LocationEmployee"));
            Long facebookLikes = (record.get("FacebookLikes") == null) ? null : ((Long) record.get("FacebookLikes"));
            Long twitterFollowers = (record.get("TwitterFollowers") == null) ? null
                    : ((Long) record.get("TwitterFollowers"));
            Long totalAmountRaised = (record.get("TotalAmountRaised") == null) ? null
                    : ((Long) record.get("TotalAmountRaised"));
            Long lastFundingRoundAmount = (record.get("LastFundingRoundAmount") == null) ? null
                    : ((Long) record.get("LastFundingRoundAmount"));
            Long searchRank = (record.get("SearchRank") == null) ? null : ((Long) record.get("SearchRank"));
            String consolidateEmployeeRange = String.valueOf(record.get("ConsolidateEmployeeRange"));
            String consolidateRevenueRange = String.valueOf(record.get("ConsolidateRevenueRange"));
            String primaryIndustry = String.valueOf(record.get("PrimaryIndustry"));
            String domain = String.valueOf(record.get("Domain"));
            String primaryDomain = String.valueOf(record.get("PrimaryDomain"));
            Boolean isSecondaryDomain = (record.get("IsSecondaryDomain") == null) ? null
                    : ((Boolean) record.get("IsSecondaryDomain"));
            Boolean domainHasEmail = (record.get("DomainHasEmail") == null) ? null
                    : ((Boolean) record.get("DomainHasEmail"));
            log.info(String
                    .format("OrbNum = %s, Name = %s, EntityType = %s, Employee = %d, LocationEmployee = %d, FacebookLikes = %d, "
                            + "TwitterFollowers = %d, TotalAmountRaised = %d, LastFundingRoundAmount = %d, SearchRank = %d, ConsolidateEmployeeRange = %s, "
                            + "ConsolidateRevenueRange = %s, PrimaryIndustry = %s, Domain = %s, PrimaryDomain = %s, IsSecondaryDomain = %s, DomainHasEmail = %s",
                    orbNum, name, entityType, employee, locationEmployee, facebookLikes, twitterFollowers,
                    totalAmountRaised, lastFundingRoundAmount, searchRank, consolidateEmployeeRange,
                    consolidateRevenueRange, primaryIndustry, domain, primaryDomain,
                    isSecondaryDomain == null ? null : String.valueOf(domainHasEmail),
                    domainHasEmail == null ? null : String.valueOf(domainHasEmail)));
            boolean flag = false;
            for (Object[] data : expectedData) {
                if (orbNum.equals(data[0]) && name.equals(data[1]) && entityType.equals(data[2])
                        && ((employee == null && data[3] == null) || employee.equals(data[3]))
                        && ((locationEmployee == null && data[4] == null) || locationEmployee.equals(data[4]))
                        && ((facebookLikes == null && data[5] == null) || facebookLikes.equals(data[5]))
                        && ((twitterFollowers == null && data[6] == null) || twitterFollowers.equals(data[6]))
                        && ((totalAmountRaised == null && data[7] == null) || totalAmountRaised.equals(data[7]))
                        && ((lastFundingRoundAmount == null && data[8] == null)
                                || lastFundingRoundAmount.equals(data[8]))
                        && ((searchRank == null && data[9] == null) || searchRank.equals(data[9]))
                        && consolidateEmployeeRange.equals(data[10])
                        && consolidateRevenueRange.equals(data[11]) && primaryIndustry.equals(data[12])
                        && domain.equals(data[13]) && primaryDomain.equals(data[14]) && isSecondaryDomain == data[15]
                        && domainHasEmail == data[16]) {
                    flag = true;
                    break;
                }
            }
            Assert.assertTrue(flag);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 12);

    }
}

