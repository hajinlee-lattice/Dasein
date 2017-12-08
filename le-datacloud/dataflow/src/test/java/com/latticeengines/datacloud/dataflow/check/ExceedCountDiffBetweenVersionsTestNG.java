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
import com.latticeengines.domain.exposed.datacloud.check.ExceedCntDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;

public class ExceedCountDiffBetweenVersionsTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(ExceedCountDiffBetweenVersionsTestNG.class);
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
                { "L628", "visa.com", "DUNS91" }, //
                { "L629", "krux.com", "DUNS92" }, //
                { "L620", "datos.com", "DUNS93" }, //
                { "L621", "mcfee.com", "DUNS94" }, //
                { "L622", "firefox.com", "DUNS95" }, //
                { "L623", "data.com", "DUNS96" }, //
                { "L624", "brightside.com", "DUNS97" }, //
                { "L625", "pixel.com", "DUNS98" }, //
                { "L626", "airbnb.com", "DUNS99" }, //
        };
        uploadAvro(data2, fields, DATA2, "/tmp/data2");

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(AVRO_INPUT, DATA2));
        ExceedCntDiffBetwenVersionChkParam checkParam = new ExceedCntDiffBetwenVersionChkParam();
        checkParam.setKeyField("LatticeID");
        checkParam.setThreshold(5.0);
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
            log.info("Check Code : " + record.get(DataCloudConstants.CHK_ATTR_CHK_CODE) + " CheckField : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_FIELD) + " CheckValue : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE) + " CheckMessage : "
                    + record.get(DataCloudConstants.CHK_ATTR_CHK_MSG));
            double countValue = Double.parseDouble(record.get(DataCloudConstants.CHK_ATTR_CHK_VALUE).toString());
            Assert.assertEquals(78.26, countValue);
        }
    }

}
