package com.latticeengines.domain.exposed.datacloud.match;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DunsGuideBookUnitTestNG {

    private static final String RECORD_TYPE = "DunsGuideBook2.0.9_20180104";
    private static final String SRC_DUNS = "fake_source_duns";
    private static final String TARGET_DUNS_PREFIX = "fake_target_duns_";
    private static final FabricEntityTestUtils.CompressAlgo algo = FabricEntityTestUtils.CompressAlgo.SNAPPY;

    /*
     * test serialization and deserialization
     */
    @Test(groups = "unit", dataProvider = "dunsGuideBookSerDe")
    public void testSerde(DunsGuideBook book, boolean shouldCompress, String recordType) {
        ByteBuffer buf  = FabricEntityTestUtils.avroToBytes(book.toFabricAvroRecord(recordType), shouldCompress, algo);
        Assert.assertNotNull(buf);

        Schema schema = book.getSchema(recordType);
        Assert.assertNotNull(schema);

        GenericRecord record = FabricEntityTestUtils.bytesToAvro(buf.array(), schema, shouldCompress, algo);
        Assert.assertNotNull(record);
        DunsGuideBook result = new DunsGuideBook();
        // should be empty before deserialization
        Assert.assertNull(result.getItems());
        Assert.assertNull(result.getItems());
        result = result.fromFabricAvroRecord(record);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getId(), book.getId());
        Assert.assertNotNull(result.getItems());
        // null will be changed into an empty list before serialization for safety
        Assert.assertEquals(result.getItems().size(), book.getItems() == null ? 0 : book.getItems().size());
        // patched field will NOT be serialized to avro
        Assert.assertFalse(result.isPatched());

        // verify each item
        for (int i = 0; i < result.getItems().size(); i++) {
            DunsGuideBook.Item item = result.getItems().get(i);
            DunsGuideBook.Item expectedItem = book.getItems().get(i);
            Assert.assertNotNull(item);
            Assert.assertEquals(item.getDuns(), expectedItem.getDuns());
            Assert.assertEquals(item.getKeyPartition(), expectedItem.getKeyPartition());
            Assert.assertEquals(item.getPatched(), expectedItem.getPatched());
        }
    }

    @Test(groups = "unit")
    public void testBackwardCompatibility() {
        // DunsGuideBook without new fields
        DunsGuideBook book = new DunsGuideBook();
        book.setId(SRC_DUNS);
        DunsGuideBook.Item item = new DunsGuideBook.Item();
        item.setDuns(TARGET_DUNS_PREFIX);
        item.setKeyPartition(MatchKey.DUNS.name());
        // old entry does not have patched field in book & item
        book.setPatched(null);
        item.setPatched(null);
        book.setItems(Collections.singletonList(item));

        ByteBuffer buf  = FabricEntityTestUtils
                .avroToBytes(book.toFabricAvroRecord(RECORD_TYPE), true, algo);
        Assert.assertNotNull(buf);
        GenericRecord record = FabricEntityTestUtils
                .bytesToAvro(buf.array(), book.getSchema(RECORD_TYPE), true, algo);
        Assert.assertNotNull(record);

        DunsGuideBook result = new DunsGuideBook();
        try {
            result = result.fromFabricAvroRecord(record);
        } catch (Exception e) {
            Assert.fail("Should be able to deserialize from legacy avro record, err=" + e.getMessage());
        }

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getItems());
        Assert.assertEquals(result.getItems().size(), 1);
        DunsGuideBook.Item resItem = result.getItems().get(0);
        Assert.assertNotNull(resItem);
        // should get the default value
        Assert.assertEquals(resItem.getPatched(), Boolean.FALSE);
        Assert.assertFalse(result.isPatched());
    }

    @DataProvider(name = "dunsGuideBookSerDe")
    public Object[][] provideSerdeTestObjs() {
        return new Object[][] {
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, -1), false, RECORD_TYPE },
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, -1), true, RECORD_TYPE },
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, 0), false, RECORD_TYPE },
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, 0), true, RECORD_TYPE },
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, 1), false, RECORD_TYPE },
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, 1), true, RECORD_TYPE },
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, 3), false, RECORD_TYPE },
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, 3), true, RECORD_TYPE },
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, 10), false, RECORD_TYPE },
                { newBook(SRC_DUNS, TARGET_DUNS_PREFIX, 10), true, RECORD_TYPE },
        };
    }

    /*
     * nItems < 0 means null list
     * nItems >= 0 means non-null list with size = nItems
     */
    private DunsGuideBook newBook(String srcDuns, String targetDunsPrefix, int nItems) {
        DunsGuideBook book = new DunsGuideBook();
        book.setId(srcDuns);
        book.setPatched(true);
        if (nItems >= 0) {
            book.setItems(newItemList(targetDunsPrefix, nItems));
        }
        return book;
    }

    private List<DunsGuideBook.Item> newItemList(String dunsPrefix, int nItems) {
        List<DunsGuideBook.Item> items = new ArrayList<>();
        for (int i = 0; i < nItems; i++) {
            Boolean patched = null;
            if (i % 3 == 1) {
                patched = true;
            } else if (i % 3 == 2) {
                patched = false;
            }
            items.add(newItem(dunsPrefix + i, MatchKey.City, patched));
        }
        return items;
    }

    private DunsGuideBook.Item newItem(String targetDuns, MatchKey matchKey, Boolean patched) {
        DunsGuideBook.Item item = new DunsGuideBook.Item();
        item.setDuns(targetDuns);
        item.setKeyPartition(matchKey.name());
        if (patched != null) {
            item.setPatched(patched);
        }
        return item;
    }
}
