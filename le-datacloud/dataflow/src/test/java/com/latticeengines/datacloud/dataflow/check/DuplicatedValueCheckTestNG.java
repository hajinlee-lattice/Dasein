package com.latticeengines.datacloud.dataflow.check;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.DuplicatedValueCheckParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class DuplicatedValueCheckTestNG extends DataCloudDataFlowFunctionalTestNGBase {

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
                { 2, "key2" }, //
                { 3, "key3" }, //
                { 4, "key2" }, //
                { 5, "key1" }, //
                { 6, "key1" }, //
                { 7, "key4" }, //
        };

        uploadDataToSharedAvroInput(data, fields);
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));

        DuplicatedValueCheckParam checkParam = new DuplicatedValueCheckParam();
        checkParam.setGroupByFields(Collections.singletonList("Key"));
        TestCheckConfig config = new TestCheckConfig(checkParam);
        parameters.setConfJson(JsonUtils.serialize(config));
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            String groupId = record.get(DataCloudConstants.CHK_ATTR_GROUP_ID).toString();
            long occurence = Long.valueOf(record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE).toString());
            if ("key1".endsWith(groupId)) {
                Assert.assertEquals(occurence, 3);
            }
            if ("key2".endsWith(groupId)) {
                Assert.assertEquals(occurence, 2);
            }
        }
    }

}
