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
import com.latticeengines.domain.exposed.datacloud.check.EmptyFieldCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class EmptyFieldCheckTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(EmptyFieldCheckTestNG.class);

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
                { 1, "key1" }, //
                { 2, null }, //
                { 3, "key3" }, //
                { 4, "key2" }, //
                { 5, "key1" }, //
                { 6, null }, //
                { 7, "key4" }, //
        };

        uploadDataToSharedAvroInput(data, fields);
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));

        EmptyFieldCheckParam checkParam = new EmptyFieldCheckParam();
        checkParam.setCheckEmptyField("Key");
        checkParam.setKeyField("Id");
        TestCheckConfig config = new TestCheckConfig(checkParam);
        parameters.setConfJson(JsonUtils.serialize(config));
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        String[] expectedId = { "2", "6" };
        String[] actualId = new String[2];
        int i = 0;
        for (GenericRecord record : records) {
            log.info("Check Code : " + record.get(DataCloudConstants.CHK_ATTR_CHK_CODE) + " RowId : "
                    + record.get(DataCloudConstants.CHK_ATTR_ROW_ID) + " GroupId : "
                    + record.get(DataCloudConstants.CHK_ATTR_GROUP_ID) + " CheckField : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_FIELD) + " CheckValue : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE) + " CheckMessage : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_MSG));
            String rowId = record.get(DataCloudConstants.CHK_ATTR_ROW_ID).toString();
            actualId[i++] = rowId;
        }
        Assert.assertEquals(actualId[0], expectedId[0]);
        Assert.assertEquals(actualId[1], expectedId[1]);
    }
}
