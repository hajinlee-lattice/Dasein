package com.latticeengines.datacloud.dataflow.check;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.check.ExceedDomDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class ExceedDomDiffBetweenVersionsTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(ExceedDomDiffBetweenVersionsTestNG.class);
    protected static final String DATA2 = "data2";

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
                Pair.of("LatticeID", String.class), //
                Pair.of("LDC_Domain", String.class), //
                Pair.of("LDC_DUNS", String.class));
        Object[][] data1 = new Object[][] { //
                { "L001", "google.com", null }, //
                { "L003", "kaggle.com", "" }, //
                { "L004", "apple.com", null }, //
                { null, "airbnb.com", "DUNS11" }, //
                { "L891", null, null }, //
                { null, "netapp.com", "DUNS14" }, //
                { "L182", "abc.xz", "" }, //
        };
        uploadDataToSharedAvroInput(data1, fields);
        Object[][] data2 = new Object[][] { //
                { "L001", "google.com", null }, //
                { "L003", "kaggle.com", "" }, //
                { "L004", "apple.com", null }, //
                { null, "airbnb.com", "DUNS11" }, //
                { "L891", null, null }, //
                { null, "netapp.com", "DUNS14" }, //
                { "L182", "abc.xz", "" }, //
                { "L627", "visa.com", "DUNS99" }, //
                { "L251", "coupa.com", "" }, //
                { "L252", "conviva.com", "" }, //
                { "L253", "groupon.com", "" }, //
                { "L254", "rei.com", "" }, //
                { "L255", "disney.com", "" }, //
                { "L256", "mmba.com", "" },
        };
        uploadAvro(data2, fields, DATA2, "/tmp/data2");

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(AVRO_INPUT, DATA2));
        ExceedDomDiffBetwenVersionChkParam checkParam = new ExceedDomDiffBetwenVersionChkParam();
        checkParam.setKeyField("LatticeID");
        checkParam.setPrevVersionNotNullField("LDC_Domain");
        checkParam.setPrevVersionNullField("LDC_DUNS");
        checkParam.setCurrVersionNotNullField("LDC_Domain");
        checkParam.setCurrVersionNullField("LDC_DUNS");
        TestCheckConfig config = new TestCheckConfig(checkParam);
        parameters.setConfJson(JsonUtils.serialize(config));
        return parameters;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return Collections.singletonMap(DATA2, "/tmp/data2/" + DATA2 + ".avro");
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            log.info("Check Code : " + record.get(DataCloudConstants.CHK_ATTR_CHK_CODE) + " GroupId : "
                    + record.get(DataCloudConstants.CHK_ATTR_GROUP_ID) + " CheckField : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_FIELD) + " CheckValue : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE) + " CheckMessage : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_MSG));
            double countValue = Double.parseDouble(record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE).toString());
            Assert.assertEquals(85.71, countValue);
        }
    }

}
