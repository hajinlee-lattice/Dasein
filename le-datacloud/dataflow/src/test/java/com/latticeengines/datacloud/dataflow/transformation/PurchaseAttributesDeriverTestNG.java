package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PurchaseAttributesDeriverConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class PurchaseAttributesDeriverTestNG extends DataCloudDataFlowFunctionalTestNGBase {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PurchaseAttributesDeriverTestNG.class);

    private String[] periodTrxFields = new String[] {
            InterfaceName.PeriodId.name(), InterfaceName.TransactionType.name(), InterfaceName.AccountId.name(),
            InterfaceName.TotalAmount.name(), InterfaceName.TotalQuantity.name(), InterfaceName.TotalCost.name(),
            InterfaceName.CDLUpdatedTime.name()
    };

    private Object[][] periodTrxData = new Object[][] {
            { 1, "PeriodTransaction", "1001", 10000.0, 10L, 6000.0, 1502755200000L },
            { 2, "PeriodTransaction", "1002", 20000.0, 10L, 15000.0, 1502755200000L },
            { 3, "PeriodTransaction", "1003", 40000.0, 20L, 30000.0, 1503001576000L },
            { 4, "PeriodTransaction", "1004", 4000.0, 30L, 1000.0, 1503001577000L },
            { 5, "PeriodTransaction", "1005", 15000.0, 50L, 10000.0, 1503001578000L },
            { 6, "PeriodTransaction", "1006", 10000.0, 10L, 3500.0, 1502755200000L },
            { 7, "PeriodTransaction", "1007", 500000.0, 100L, 350000.0, 1502755200000L },
            { 8, "PeriodTransaction", "1008", 3000.0, 300L, 2000.0, 1503001576000L },
            { 9, "PeriodTransaction", "1009", 20000.0, 40L, 10000.0, 1503001578000L },
            { 10, "PeriodTransaction", "1010", null, 50L, null, 1503001578000L },
            { 11, "PeriodTransaction", "1011", null, 50L, 1000.0, 1503001578000L },
            { 12, "PeriodTransaction", "1012", 20000.0, 50L, null, 1503001578000L },
            { 13, "PeriodTransaction", "1013", 0.0, 0L, 0.0, 1503001578000L },
    };

    private List<InterfaceName> fields = Arrays.asList(InterfaceName.Margin, InterfaceName.HasPurchased);

    private List<Double> expectedMargins = Arrays.asList(0.4, 0.25, 0.25, 0.75, 1.0/3.0, 0.65, 0.3, 1.0/3.0, 0.5,
            null, null, null, null);
    private List<Boolean> expectedHasPurchased = Arrays.asList(
            true, true, true, true, true, true,
            true, true, true, null, null, true, false);

    @Test(groups = "functional")
    public void test() {
        TransformationFlowParameters parameters = prepareInputData();
        executeDataFlow(parameters);
        verifyWithAvro();
    }

    @Override
    protected String getFlowBeanName() {
        return PurchaseAttributesDeriverFlow.FLOW_BEAN_NAME;
    }

    private TransformationFlowParameters prepareInputData() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(periodTrxFields[0], Integer.class));
        columns.add(Pair.of(periodTrxFields[1], String.class));
        columns.add(Pair.of(periodTrxFields[2], String.class));
        columns.add(Pair.of(periodTrxFields[3], Double.class));
        columns.add(Pair.of(periodTrxFields[4], Long.class));
        columns.add(Pair.of(periodTrxFields[5], Double.class));
        columns.add(Pair.of(periodTrxFields[6], Long.class));
        uploadDataToSharedAvroInput(periodTrxData, columns);

        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Collections.singletonList(AVRO_INPUT));
        parameters.setConfJson(JsonUtils.serialize(getPurchaseAttributesDeriverConfig()));
        return parameters;
    }

    private PurchaseAttributesDeriverConfig getPurchaseAttributesDeriverConfig() {
        PurchaseAttributesDeriverConfig config = new PurchaseAttributesDeriverConfig();
        config.setFields(fields);

        return config;
    }

    private void verifyWithAvro() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 13);

        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            String trxType = String.valueOf(record.get(InterfaceName.TransactionType.name()));
            Assert.assertEquals(trxType, "PeriodTransaction");

            Assert.assertEquals(record.get(InterfaceName.Margin.name()), expectedMargins.get(i));
            Assert.assertEquals(record.get(InterfaceName.HasPurchased.name()), expectedHasPurchased.get(i));
        }
    }
}
