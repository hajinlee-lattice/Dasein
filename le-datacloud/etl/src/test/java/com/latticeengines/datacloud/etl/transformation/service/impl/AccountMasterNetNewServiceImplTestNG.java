package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.AccountMaster;
import com.latticeengines.datacloud.core.source.impl.PipelineSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccountMasterNetNewConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class AccountMasterNetNewServiceImplTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(AccountMasterNetNewServiceImplTestNG.class);

    private static final String EMPLOYEE = "LE_EMPLOYEE_RANGE";

    private static final String REVENUE = "LE_REVENUE_RANGE";

    private static final String INDUSTRY = "LE_INDUSTRY";

    private static final String COUNTRY = "LE_COUNTRY";

    @Autowired
    PipelineSource source;

    @Autowired
    AccountMaster baseSource;

    String targetSourceName = "AccountMasterNetNew";

    @Test(groups = "deployment", enabled = true)
    public void testTransformation() {
        uploadBaseSourceFile(baseSource, baseSource.getSourceName() + "_Test" + targetSourceName,
                "2017-01-0_19-12-4_UTC");

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

    @SuppressWarnings("deprecation")
    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(baseSource, baseSourceVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();

        ObjectMapper om = new ObjectMapper();

        ///////////////////

        TransformationStepConfig step = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<String>();
        baseSources.add("AccountMaster");
        step.setBaseSources(baseSources);
        step.setTargetSource(targetSourceName);
        step.setTransformer("accountMasterNetNewTransformer");

        AccountMasterNetNewConfig confParam = getAccountMasterNetNewParameters();
        String confParamStr = null;
        try {
            confParamStr = om.writeValueAsString(confParam);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        step.setConfiguration(confParamStr);

        //////////////////

        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step);

        configuration.setSteps(steps);

        configuration.setVersion(HdfsPathBuilder.dateFormat.format(new Date()));

        try {
            System.out.println(om.writeValueAsString(configuration));
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return configuration;
    }

    private AccountMasterNetNewConfig getAccountMasterNetNewParameters() {
        AccountMasterNetNewConfig param = new AccountMasterNetNewConfig();

        Map<String, List<String>> filterCriteria = new HashMap<>();
        filterCriteria.put(COUNTRY, Arrays.asList("USA", "SCOTLAND", "BELGIUM"));
        filterCriteria.put(INDUSTRY, Arrays.asList("Electric Services", //
                "Used Merchandise Stores", "General Government, Nec"));
        filterCriteria.put(REVENUE, Arrays.asList("101-250M", "501M-1B", ">10B"));
        filterCriteria.put(EMPLOYEE, Arrays.asList("1001-2500", "2501-5000"));

        List<String> groupBy = Arrays.asList(COUNTRY, INDUSTRY, REVENUE, EMPLOYEE);

        List<String> resultFields = Arrays.asList(COUNTRY, INDUSTRY, REVENUE, EMPLOYEE, //
                "LatticeID", "LDC_Domain", "LDC_Name", "LDC_City", "LDC_State", //
                "LE_COMPANY_PHONE", "LE_IS_PRIMARY_DOMAIN");

        param.setFilterCriteria(filterCriteria);
        param.setGroupBy(groupBy);
        param.setResultFields(resultFields);
        return param;
    }

    @SuppressWarnings("deprecation")
    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSource, targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        Object[][] expectedData = {
                { "101-250M", "N H S LANARKSHIRE", "General Government, Nec", "gov.scot", "1001-2500", 105098484L,
                        "COATBRIDGE", "1315568400", "LANARKSHIRE", "SCOTLAND", "Y" },
                { ">10B", "Electrabel SA", "Electric Services", "electrabel.be", "2501-5000", 109977375L, "Antwerpen",
                        "25186400", "ANTWERPEN", "BELGIUM", "Y" } };
        while (records.hasNext()) {
            GenericRecord record = records.next();
            String[] fieldNames = new String[] { "LE_REVENUE_RANGE", "LDC_Name", "LE_INDUSTRY", "LDC_Domain",
                    "LE_EMPLOYEE_RANGE", "LatticeID", "LDC_City", "LE_COMPANY_PHONE", "LDC_State", "LE_COUNTRY",
                    "LE_IS_PRIMARY_DOMAIN", };

            boolean foundMatchingRecord = false;
            for (Object[] data : expectedData) {
                int idx = 0;
                boolean hasFieldMismatchInRecord = false;
                for (String fieldName : fieldNames) {
                    Object val = record.get(fieldName);
                    if (val instanceof Utf8) {
                        val = ((Utf8) val).toString();
                    }
                    Object expectedVal = data[idx];
                    System.out.print("[" + val + " - " + expectedVal + "], ");
                    if (!val.equals(expectedVal)) {
                        hasFieldMismatchInRecord = true;
                        break;
                    }
                    idx++;
                }

                if (!hasFieldMismatchInRecord //
                        || idx == fieldNames.length) {
                    foundMatchingRecord = true;
                    break;
                }
            }
            Assert.assertTrue(foundMatchingRecord);
            rowNum++;
        }
        Assert.assertEquals(rowNum, 2);
    }
}
