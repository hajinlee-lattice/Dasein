package com.latticeengines.serviceflows.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.serviceflows.workflow.export.ExportData;

public class ExportDataUnitTestNG {

    @Test(groups = "unit")
    public void testGenerateNewRecords() {
        Map<String, Attribute> attributeMap = getImportedAttributes();
        Map<String, Integer> namePosMap = getNamePositionMap();
        Map<String, Integer> displayNamePosMap = getDisplayNamePositionMap();
        List<String[]> oldRecords = getOldRecords();

        ExportData exportData = new ExportData();
        oldRecords.forEach(record -> {
            String[] newRecord = exportData.generateNewRecord(record, displayNamePosMap.keySet().size(),
                    namePosMap, displayNamePosMap, attributeMap);
            Assert.assertEquals(newRecord[3], "1/15/2019");
            Assert.assertEquals(newRecord[4], "1");
            Assert.assertEquals(newRecord[5], "100.0");
            Assert.assertEquals(newRecord[6], "2019");
            Assert.assertEquals(newRecord[7], "Name_001");
        });
    }

    private Map<String, Attribute> getImportedAttributes() {
        Map<String, Attribute> attributeMap = new HashMap<>();

        Attribute accountIdAttribute = new Attribute();
        accountIdAttribute.setName("AccountId");
        accountIdAttribute.setDisplayName("Account ID");
        accountIdAttribute.setLogicalDataType(LogicalDataType.Id);
        attributeMap.put(accountIdAttribute.getName(), accountIdAttribute);

        Attribute productIdAttribute = new Attribute();
        productIdAttribute.setName("ProductId");
        productIdAttribute.setDisplayName("SKU ID");
        productIdAttribute.setLogicalDataType(LogicalDataType.Id);
        attributeMap.put(productIdAttribute.getName(), productIdAttribute);

        Attribute orderIdAttribute = new Attribute();
        orderIdAttribute.setName("OrderId");
        orderIdAttribute.setDisplayName("Order ID");
        orderIdAttribute.setLogicalDataType(LogicalDataType.Id);
        attributeMap.put(orderIdAttribute.getName(), orderIdAttribute);

        Attribute quantityAttribute = new Attribute();
        quantityAttribute.setName("Quantity");
        quantityAttribute.setDisplayName("Quantity");
        quantityAttribute.setLogicalDataType(LogicalDataType.Metric);
        attributeMap.put(quantityAttribute.getName(), quantityAttribute);

        Attribute amountAttribute = new Attribute();
        amountAttribute.setName("Amount");
        amountAttribute.setDisplayName("Amount");
        amountAttribute.setLogicalDataType(LogicalDataType.Metric);
        attributeMap.put(amountAttribute.getName(), amountAttribute);

        Attribute trxTimeAttribute = new Attribute();
        trxTimeAttribute.setName("TransactionTime");
        trxTimeAttribute.setDisplayName("Sale Date");
        trxTimeAttribute.setLogicalDataType(LogicalDataType.Timestamp);
        attributeMap.put(trxTimeAttribute.getName(), trxTimeAttribute);

        return attributeMap;
    }

    private Map<String, Integer> getNamePositionMap() {
        Map<String, Integer> namePosMap = new HashMap<>();
        namePosMap.put("AccountId", 0);
        namePosMap.put("ProductId", 1);
        namePosMap.put("OrderId", 2);
        namePosMap.put("Quantity", 3);
        namePosMap.put("Amount", 4);
        namePosMap.put("TransactionTime", 5);
        namePosMap.put("CDLGenerated_1", 6);
        namePosMap.put("CDLGenerated_2", 7);
        namePosMap.put("CDLGenerated_3", 8);
        namePosMap.put("CustomTrxField", 9);
        namePosMap.put("ContactId", 10);
        return namePosMap;
    }

    private Map<String, Integer> getDisplayNamePositionMap() {
        Map<String, Integer> displayNamePosMap = new HashMap<>();
        displayNamePosMap.put("Account ID", 0);
        displayNamePosMap.put("Order ID", 1);
        displayNamePosMap.put("SKU ID", 2);
        displayNamePosMap.put("Sale Date", 3);
        displayNamePosMap.put("Quantity", 4);
        displayNamePosMap.put("Amount", 5);
        displayNamePosMap.put("Sale Year", 6);
        displayNamePosMap.put("Account Name", 7);
        return displayNamePosMap;
    }

    private List<String[]> getOldRecords() {
        final Long TRX_TIMESTAMP = 1547583422000L;  // 1/15/19 in both UTC and PST
        List<String[]> oldRecords = new ArrayList<>();
        oldRecords.add(new String[]{"A001", "P001", "T001", "1", "100.0", String.valueOf(TRX_TIMESTAMP),
                "CDLGenerated_1", "CDLGenerated_2", "CDLGenerated_3",
                "{\"user_Account_Name\": \"Name_001\", \"user_Sale_Year\": \"2019\"}", null});
        oldRecords.add(new String[]{"A002", "P001", "T002", "1", "100.0", String.valueOf(TRX_TIMESTAMP),
                "CDLGenerated_1", "CDLGenerated_2", "CDLGenerated_3",
                "{\"user_Account_Name\": \"Name_001\", \"user_Sale_Year\": \"2019\"}", null});
        oldRecords.add(new String[]{"A003", "P003", "T003", "1", "100.0", String.valueOf(TRX_TIMESTAMP),
                "CDLGenerated_1", "CDLGenerated_2", "CDLGenerated_3",
                "{\"user_Account_Name\": \"Name_001\", \"user_Sale_Year\": \"2019\"}", null});
        oldRecords.add(new String[]{"A099", "P099", "T004", "1", "100.0", String.valueOf(TRX_TIMESTAMP),
                "CDLGenerated_1", "CDLGenerated_2", "CDLGenerated_3",
                "{\"user_Account_Name\": \"Name_001\", \"user_Sale_Year\": \"2019\"}", null});
        oldRecords.add(new String[] {"A100", "P100", "T005", "1", "100.0", String.valueOf(TRX_TIMESTAMP),
                "CDLGenerated_1", "CDLGenerated_2", "CDLGenerated_3",
                "{\"user_Account_Name\": \"Name_001\", \"user_Sale_Year\": \"2019\"}", null});
        return oldRecords;
    }
}
