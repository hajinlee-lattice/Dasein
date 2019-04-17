package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.DnBCacheSeed;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateIndustryStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.StandardizationStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DnBStandardizeTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(DnBStandardizeTestNG.class);

    @Autowired
    DnBCacheSeed source;

    GeneralSource baseSource = new GeneralSource("DnBCacheSeedRaw");

    String targetSourceName = "DnBCacheSeed";

    ObjectMapper om = new ObjectMapper();

    private static final String DUNS_NUMBER = "DUNS_NUMBER";
    private static final String LE_DOMAIN = "LE_DOMAIN";
    private static final String LE_PRIMARY_DUNS = "LE_PRIMARY_DUNS";
    private static final String LE_NUMBER_OF_LOCATIONS = "LE_NUMBER_OF_LOCATIONS";
    private static final String SALES_VOLUME_LOCAL_CURRENCY = "SALES_VOLUME_LOCAL_CURRENCY";
    private static final String SALES_VOLUME_US_DOLLARS = "SALES_VOLUME_US_DOLLARS";
    private static final String EMPLOYEES_HERE = "EMPLOYEES_HERE";
    private static final String EMPLOYEES_TOTAL = "EMPLOYEES_TOTAL";
    private static final String NUMBER_OF_FAMILY_MEMBERS = "NUMBER_OF_FAMILY_MEMBERS";
    private static final String LE_PRIMARY_INDUSTRY = "LE_PRIMARY_INDUSTRY";

    @Test(groups = "pipeline1", enabled = true)
    public void testTransformation() {
        uploadBaseAvro(baseSource, baseSourceVersion);
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

            configuration.setName("DnBCacheSeedClean");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(baseSource.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer("standardizationTransformer");
            step1.setTargetSource(targetSourceName);
            String confParamStr1 = getStandardizationTransformerConfig();
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

    private String getStandardizationTransformerConfig() throws JsonProcessingException {
        StandardizationTransformerConfig conf = new StandardizationTransformerConfig();
        String filterExpression = "DUNS_NUMBER != null";
        conf.setFilterExpression(filterExpression);
        String[] filterFields = { DUNS_NUMBER };
        conf.setFilterFields(filterFields);
        String[] domainFields = { LE_DOMAIN };
        conf.setDomainFields(domainFields);
        conf.setConsolidateIndustryStrategy(ConsolidateIndustryStrategy.PARSE_NAICS);
        conf.setAddConsolidatedIndustryField(LE_PRIMARY_INDUSTRY);
        conf.setNaicsField("LE_NAICS_CODE");
        conf.setNaicsMapFileName("NaicsIndustryMapping.txt");
        String[] convertTypeFields = { LE_NUMBER_OF_LOCATIONS, EMPLOYEES_HERE, EMPLOYEES_TOTAL,
                NUMBER_OF_FAMILY_MEMBERS, SALES_VOLUME_LOCAL_CURRENCY, SALES_VOLUME_US_DOLLARS };
        conf.setConvertTypeFields(convertTypeFields);
        TypeConvertStrategy[] convertTypeStrategies = { TypeConvertStrategy.STRING_TO_INT,
                TypeConvertStrategy.STRING_TO_INT, TypeConvertStrategy.STRING_TO_INT, TypeConvertStrategy.STRING_TO_INT,
                TypeConvertStrategy.STRING_TO_LONG, TypeConvertStrategy.STRING_TO_LONG };
        conf.setConvertTypeStrategies(convertTypeStrategies);
        String[] dedupFields = { DUNS_NUMBER, LE_DOMAIN };
        conf.setDedupFields(dedupFields);
        String uploadTimestampField = "LE_Last_Upload_Date";
        conf.setUploadTimestampField(uploadTimestampField);
        StandardizationTransformerConfig.StandardizationStrategy[] sequence = { StandardizationStrategy.FILTER,
                StandardizationStrategy.DOMAIN, StandardizationStrategy.CONSOLIDATE_INDUSTRY,
                StandardizationStrategy.CONVERT_TYPE, StandardizationStrategy.DEDUP,
                StandardizationStrategy.UPLOAD_TIMESTAMP };
        conf.setSequence(sequence);
        return om.writeValueAsString(conf);
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
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        Object[][] expectedData = {
                { "01", "google.com", 10, 100L, 1000L, 10000, 100000, 1000000, "Biotechnology", "01" },
                { "02", "yahoo.com", 10, 100L, 1000L, 10000, 100000, 1000000, "null", "null" } };
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String duns = String.valueOf(record.get(DUNS_NUMBER));
            String domain = String.valueOf(record.get(LE_DOMAIN));
            Integer numberOfLocation = (Integer) record.get(LE_NUMBER_OF_LOCATIONS);
            Long salesVolumnLocalCurrency = (Long) record.get(SALES_VOLUME_LOCAL_CURRENCY);
            Long salesVolumnUSDollars = (Long) record.get(SALES_VOLUME_US_DOLLARS);
            Integer employeeHere = (Integer) record.get(EMPLOYEES_HERE);
            Integer employeeTotal = (Integer) record.get(EMPLOYEES_TOTAL);
            Integer numberOfFamilyMembers = (Integer) record.get(NUMBER_OF_FAMILY_MEMBERS);
            String lePrimaryIndustry = String.valueOf(record.get(LE_PRIMARY_INDUSTRY));
            String lePrimaryDuns = String.valueOf(record.get(LE_PRIMARY_DUNS));
            log.info(DUNS_NUMBER + "=" + duns + " " + LE_DOMAIN + "=" + domain + " " + LE_NUMBER_OF_LOCATIONS + "="
                    + numberOfLocation + " " + SALES_VOLUME_LOCAL_CURRENCY + "=" + salesVolumnLocalCurrency + " "
                    + SALES_VOLUME_US_DOLLARS + "=" + salesVolumnUSDollars + " " + EMPLOYEES_HERE + "=" + employeeHere
                    + " " + EMPLOYEES_TOTAL + "=" + employeeTotal + " " + NUMBER_OF_FAMILY_MEMBERS + "="
                    + numberOfFamilyMembers + " " + " " + LE_PRIMARY_INDUSTRY + "=" + lePrimaryIndustry + " "
                    + LE_PRIMARY_DUNS + "=" + lePrimaryDuns);
            boolean flag = false;
            for (Object[] data : expectedData) {
                if (duns.equals(data[0]) && domain.equals(data[1]) && numberOfLocation.equals(data[2])
                        && salesVolumnLocalCurrency.equals(data[3]) && salesVolumnUSDollars.equals(data[4])
                        && employeeHere.equals(data[5]) && employeeTotal.equals(data[6])
                        && numberOfFamilyMembers.equals(data[7]) && lePrimaryIndustry.equals(data[8])
                        && lePrimaryDuns.equals(data[9])) {
                    flag = true;
                    break;
                }
            }
            Assert.assertTrue(flag);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 2);
    }

    @Override
    protected void uploadBaseAvro(Source baseSource, String baseSourceVersion) {
        InputStream baseAvroStream = ClassLoader
                .getSystemResourceAsStream("sources/" + baseSource.getSourceName() + ".avro");
        String targetPath = hdfsPathBuilder.constructSnapshotDir(baseSource.getSourceName(), baseSourceVersion)
                .append("part-0000.avro").toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                HdfsUtils.rmdir(yarnConfiguration, targetPath);
            }
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseAvroStream, targetPath);
            InputStream stream = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8));
            String successPath = hdfsPathBuilder.constructRawDir(baseSource).append(baseSourceVersion)
                    .append("_SUCCESS").toString();
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, stream, successPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        hdfsSourceEntityMgr.setCurrentVersion(baseSource, baseSourceVersion);
    }

}
