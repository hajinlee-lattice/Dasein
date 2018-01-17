package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CleanupConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CleanupTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    protected static final String CLEANUPBASE = "cleanup_base";


    @Override
    protected String getFlowBeanName() {
        return CleanupFlow.DATAFLOW_BEAN_NAME;
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private TransformationFlowParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("AccountId", String.class), //
                Pair.of("Key", String.class), //
                Pair.of("Domain", String.class), //
                Pair.of("DUNS", String.class) //
        );
        Object[][] data = new Object[][] { //
                { "1", "key1", "netapp.com", "DUNS11" }, //
                { "2", "key2", "netapp.com", null }, //
                { "3", "key1", "netapp.com", "DUNS11" }, //
                { "4", "key2", null, "DUNS14" }, //
                { "5", "key1", null, "DUNS14" }, //
                { "6", "key2", null, null }, //
        };

        List<Pair<String, Class<?>>> fields2 = Arrays.asList( //
                Pair.of("AccountId", String.class)//
        );
        Object[][] data2 = new Object[][] { //
                { "1" }, //
                { "3" }, //
        };
        uploadDataToSharedAvroInput(data, fields);
        uploadAvro(data2, fields2, CLEANUPBASE, "/tmp/cleanup_base");

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(AVRO_INPUT, CLEANUPBASE));
        CleanupConfig config = new CleanupConfig();
        config.setJoinColumn("AccountId");
        config.setBusinessEntity(BusinessEntity.Account);
        config.setOperationType(CleanupOperationType.BYUPLOAD_ID);
        parameters.setConfJson(JsonUtils.serialize(config));
        return parameters;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return Collections.singletonMap(CLEANUPBASE, "/tmp/cleanup_base/" + CLEANUPBASE + ".avro");
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 4);
        boolean contains1 = false;
        boolean contains3 = false;
        for (GenericRecord record : records) {
            if (record.get("AccountId").toString().equals("1")) {
                contains1 = true;
            }
            if (record.get("AccountId").toString().equals("3")) {
                contains3 = true;
            }
        }
        Assert.assertFalse(contains1 || contains3);
    }
}
