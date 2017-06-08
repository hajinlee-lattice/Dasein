
package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public class AccountMasterColumnSelectionServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static String dataCloudVersion;
    private static ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    @Qualifier("accountMasterColumnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @Autowired
    @Qualifier("accountMasterColumnEntityMgr")
    private MetadataColumnEntityMgr<AccountMasterColumn> columnEntityMgr;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Test(groups = "functional")
    public void testGetBitCodeBook() {
        dataCloudVersion = versionEntityMgr.currentApprovedVersionAsString();

        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columnList = new ArrayList<>();
        columnList.add(new Column("TechIndicator_Aastra"));
        columnList.add(new Column("TechIndicator_3GIS"));
        columnList.add(new Column("TechIndicator_Google"));
        columnList.add(new Column("TechIndicator_Apple"));
        columnList.add(new Column("TechIndicator_BidRun"));
        columnList.add(new Column("TechIndicator_Artemis"));
        columnSelection.setColumns(columnList);

        Map<String, Pair<BitCodeBook, List<String>>> parameters = columnSelectionService
                .getDecodeParameters(columnSelection, dataCloudVersion);

        Assert.assertEquals(parameters.size(), 3);

        Pair<BitCodeBook, List<String>> pair = parameters.get("BuiltWith_TechIndicators");
        Map<String, Integer> bitPosExpected = new HashMap<>();
        bitPosExpected.put("TechIndicator_BidRun", getBitPosition("TechIndicator_BidRun"));
        bitPosExpected.put("TechIndicator_Artemis", getBitPosition("TechIndicator_Artemis"));
        verifyCodeBookParameters(pair, bitPosExpected);

        pair = parameters.get("HGData_SupplierTechIndicators");
        bitPosExpected = new HashMap<>();
        bitPosExpected.put("TechIndicator_Google", getBitPosition("TechIndicator_Google"));
        bitPosExpected.put("TechIndicator_Apple", getBitPosition("TechIndicator_Apple"));
        verifyCodeBookParameters(pair, bitPosExpected);

        pair = parameters.get("HGData_SegmentTechIndicators");
        bitPosExpected = new HashMap<>();
        bitPosExpected.put("TechIndicator_Aastra", getBitPosition("TechIndicator_Aastra"));
        bitPosExpected.put("TechIndicator_3GIS", getBitPosition("TechIndicator_3GIS"));
        verifyCodeBookParameters(pair, bitPosExpected);

    }

    private void verifyCodeBookParameters(Pair<BitCodeBook, List<String>> pair, Map<String, Integer> bitPosExpected) {
        BitCodeBook codeBook = pair.getLeft();
        List<String> declaredDecodeFields = pair.getRight();

        for (Map.Entry<String, Integer> bitPos : bitPosExpected.entrySet()) {
            Integer posExpected = bitPos.getValue();
            Integer posGenerated = codeBook.getBitPosForKey(bitPos.getKey());
            Assert.assertEquals(posGenerated, posExpected,
                    String.format("Expect the bit position of %s to be %d, but found %d instead", bitPos.getKey(),
                            posExpected, posGenerated));
            Assert.assertTrue(declaredDecodeFields.contains(bitPos.getKey()),
                    "The declared decode fields do not contain " + bitPos.getKey());
        }
    }

    private int getBitPosition(String columnName) {
        try {
            AccountMasterColumn column = columnEntityMgr.findById(columnName,
                    versionEntityMgr.currentApprovedVersionAsString());
            String decodeStrategy = column.getDecodeStrategy();
            JsonNode jsonNode = objectMapper.readTree(decodeStrategy);
            return jsonNode.get("BitPosition").asInt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
