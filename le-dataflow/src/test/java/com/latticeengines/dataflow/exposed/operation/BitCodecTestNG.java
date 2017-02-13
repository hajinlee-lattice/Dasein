package com.latticeengines.dataflow.exposed.operation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

public class BitCodecTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testBitEncode() throws Exception {
        Object[][] data = new Object[][] { { "dom1.com", "f1", 1, 123L }, //
                { "dom1.com", "f2", 2, 125L }, //
                { "dom1.com", "f3", 3, 124L }, //
                { "dom2.com", "f2", 4, 101L }, //
                { "dom2.com", "f3", 2, 102L } };
        uploadDynamicSourceAvro(data, PivotTestNG.featureSchema());

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource(DYNAMIC_SOURCE);
                String[] groupbyFields = new String[] { "Domain" };
                BitCodeBook codeBook = new BitCodeBook(BitCodeBook.Algorithm.KEY_EXISTS);
                Map<String, Integer> bitPosMap = new HashMap<>();
                bitPosMap.put("f1", 0);
                bitPosMap.put("f2", 1);
                bitPosMap.put("f3", 2);
                codeBook.setBitsPosMap(bitPosMap);
                return node.bitEncode(groupbyFields, "Feature", null, "Encoded", codeBook);
            }
        });

        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            System.out.println(record);
            String encoded = record.get("Encoded").toString();
            boolean[] bits = BitCodecUtils.decode(encoded, new int[] { 0, 1, 2, 3 });
            String domain = record.get("Domain").toString();
            if ("dom1.com".equals(domain)) {
                Assert.assertTrue(bits[0]);
                Assert.assertTrue(bits[1]);
                Assert.assertTrue(bits[2]);
                Assert.assertFalse(bits[3]);
            } else if ("dom1.com".equals(domain)) {
                Assert.assertFalse(bits[0]);
                Assert.assertTrue(bits[1]);
                Assert.assertTrue(bits[2]);
                Assert.assertFalse(bits[3]);
            }
        }
    }

    @Test(groups = "functional")
    public void testBitDecode() throws Exception {
        Object[][] data = new Object[][] { { "dom1.com", BitCodecUtils.encode(new int[] { 1, 3 }), 1, 123L }, //
                { "dom2.com", BitCodecUtils.encode(new int[] { 0, 2 }), 2, 102L }, //
                { "dom3.com", null, 2, 102L } };
        uploadDynamicSourceAvro(data, PivotTestNG.featureSchema());

        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node node = addSource(DYNAMIC_SOURCE);

                BitCodeBook codebook1 = new BitCodeBook(BitCodeBook.DecodeStrategy.BOOLEAN_YESNO);
                Map<String, Integer> bitPosMap1 = new HashMap<>();
                bitPosMap1.put("f1", 0);
                bitPosMap1.put("f2", 1);
                bitPosMap1.put("f3", 2);
                bitPosMap1.put("f4", 3);
                codebook1.setBitsPosMap(bitPosMap1);

                BitCodeBook codebook2 = new BitCodeBook(BitCodeBook.DecodeStrategy.BOOLEAN_YESNO);
                Map<String, Integer> bitPosMap2 = new HashMap<>();
                bitPosMap2.put("g1", 3);
                bitPosMap2.put("g2", 2);
                bitPosMap2.put("g3", 1);
                bitPosMap2.put("g4", 0);
                codebook2.setBitsPosMap(bitPosMap2);

                node = node.bitDecode("Feature", new String[] { "f1", "f2", "f3", "f4" }, codebook1);
                node = node.bitDecode("Feature", new String[] { "g1", "g2", "g3", "g4" }, codebook2);
                node = node.retain(new FieldList("Domain", "f1", "f2", "f3", "f4", "g1", "g2", "g3", "g4"));
                return node;
            }
        });

        for (GenericRecord record : readOutput()) {
            String domain = record.get("Domain").toString();
            switch (domain) {
                case "dom1.com":
                    Assert.assertEquals(record.get("f1").toString(), "No");
                    Assert.assertEquals(record.get("f2").toString(), "Yes");
                    Assert.assertEquals(record.get("f3").toString(), "No");
                    Assert.assertEquals(record.get("f4").toString(), "Yes");
                    Assert.assertEquals(record.get("g1").toString(), "Yes");
                    Assert.assertEquals(record.get("g2").toString(), "No");
                    Assert.assertEquals(record.get("g3").toString(), "Yes");
                    Assert.assertEquals(record.get("g4").toString(), "No");
                    break;
                case "dom2.com":
                    Assert.assertEquals(record.get("f1").toString(), "Yes");
                    Assert.assertEquals(record.get("f2").toString(), "No");
                    Assert.assertEquals(record.get("f3").toString(), "Yes");
                    Assert.assertEquals(record.get("f4").toString(), "No");
                    Assert.assertEquals(record.get("g1").toString(), "No");
                    Assert.assertEquals(record.get("g2").toString(), "Yes");
                    Assert.assertEquals(record.get("g3").toString(), "No");
                    Assert.assertEquals(record.get("g4").toString(), "Yes");
                    break;
                case "dom3.com":
                    Assert.assertNull(record.get("f1"));
                    Assert.assertNull(record.get("f2"));
                    Assert.assertNull(record.get("f3"));
                    Assert.assertNull(record.get("f4"));
                    Assert.assertNull(record.get("g1"));
                    Assert.assertNull(record.get("g2"));
                    Assert.assertNull(record.get("g3"));
                    Assert.assertNull(record.get("g4"));
                    break;
            }
        }
    }

}
