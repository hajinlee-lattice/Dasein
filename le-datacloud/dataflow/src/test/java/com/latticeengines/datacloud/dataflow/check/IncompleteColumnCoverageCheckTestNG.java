package com.latticeengines.datacloud.dataflow.check;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.IncompleteCoverageForColCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class IncompleteColumnCoverageCheckTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(IncompleteColumnCoverageCheckTestNG.class);

    @Override
    protected String getFlowBeanName() {
        return TestChecker.DATAFLOW_BEAN;
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private TransformationFlowParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("Key", String.class) //
        );
        Object[][] data = new Object[][] { //
                { 1, "RTS" }, //
                { 2, "RTS" }, //
                { 3, "HG" }, //
                { 4, "Orb" }, //
                { 5, "Orb" }, //
                { 6, "RTS" }, //
                { 7, "HG" }, //
        };

        uploadDataToSharedAvroInput(data, fields);
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));

        IncompleteCoverageForColCheckParam checkParam = new IncompleteCoverageForColCheckParam();
        checkParam.setGroupByFields(Collections.singletonList("Key"));
        Object[] fieldsArray = new Object[] { "DnB", "RTS", "HG", "Orb", "Manual" };
        List<Object> expectedFieldValues = Arrays.asList(fieldsArray);
        checkParam.setExpectedFieldValues(expectedFieldValues);
        TestCheckConfig config = new TestCheckConfig(checkParam);
        parameters.setConfJson(JsonUtils.serialize(config));
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        String[] expected = new String[2];
        expected[0] = "DnB";
        expected[1] = "Manual";
        for (GenericRecord record : records) {
            log.info("Check Code : " + record.get(DataCloudConstants.CHK_ATTR_CHK_CODE) + " RowId : "
                    + record.get(DataCloudConstants.CHK_ATTR_ROW_ID) + " GroupId : "
                    + record.get(DataCloudConstants.CHK_ATTR_GROUP_ID) + " CheckField : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_FIELD) + " CheckValue : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE) + " CheckMessage : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_MSG));
            String missingList = record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE).toString();
            String[] missingArr = missingList.split(",");
            Assert.assertEquals(missingArr[0].trim(), expected[0]);
            Assert.assertEquals(missingArr[1].trim(), expected[1]);
        }
    }

}
