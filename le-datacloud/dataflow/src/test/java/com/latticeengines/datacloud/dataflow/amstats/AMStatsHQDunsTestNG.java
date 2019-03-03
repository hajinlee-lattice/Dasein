package com.latticeengines.datacloud.dataflow.amstats;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.LATTICE_ACCOUNT_ID;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.LATTICE_ID;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.DDUNS;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.DOMAIN;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.DUNS;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.GDUNS;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.HQ_DUNS;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.STATUS_CODE;
import static com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters.SUBSIDIARY_INDICATOR;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.datacloud.dataflow.transformation.AMStatsHQDuns;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public class AMStatsHQDunsTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return AMStatsHQDuns.BEAN_NAME;
    }

    @Test(groups = "functional")
    public void test() throws Exception {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private TransformationFlowParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of(LATTICE_ID, Integer.class), //
                Pair.of(STATUS_CODE, String.class), //
                Pair.of(SUBSIDIARY_INDICATOR, String.class), //
                Pair.of(DUNS, String.class), //
                Pair.of(DDUNS, String.class), //
                Pair.of(GDUNS, String.class), //
                Pair.of(DOMAIN, String.class) //
        );
        Object[][] data = new Object[][] { //
                { 1, "0", "0", "duns", "dduns", "gduns", "1.com" }, // hqduns = duns
                { 2, "1", "0", "duns", "dduns", "gduns", "2.com" }, // hqduns = dduns
                { 3, "1", "0", "duns", null, "gduns", "3.com" },    // hqduns = gduns
                { 4, "1", "3", "duns", "dduns", "gduns", "4.com" }, // hqduns = duns
                { 5, "0", "3", "duns", "duns", "gduns", "5.com" },  // hqduns = duns
                { 6, "0", "3", "duns", "dduns", "gduns", "6.com" }, // hqduns = null
        };

        uploadDataToSharedAvroInput(data, fields);

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.setConfJson(JsonUtils.serialize(new TransformerConfig()));
        return parameters;
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            System.out.println(record);
            String hqduns = record.get(HQ_DUNS) == null ? null : record.get(HQ_DUNS).toString();
            switch ((int) record.get(LATTICE_ACCOUNT_ID)) {
                case 1:
                    Assert.assertEquals(hqduns, "duns");
                    break;
                case 2:
                    Assert.assertEquals(hqduns, "dduns");
                    break;
                case 3:
                    Assert.assertEquals(hqduns, "gduns");
                    break;
                case 4:
                    Assert.assertEquals(hqduns, "duns");
                    break;
                case 5:
                    Assert.assertEquals(hqduns, "duns");
                    break;
                case 6:
                    Assert.assertEquals(hqduns, null);
                    break;
            }
        }
    }

}
