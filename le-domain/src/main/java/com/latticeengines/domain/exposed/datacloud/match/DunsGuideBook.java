package com.latticeengines.domain.exposed.datacloud.match;

import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datafabric.BaseFabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

/**
 * Used to redirect source duns to a target duns that is obtained by a more accurate match key.
 *
 * 1. Input of DunsGuideBook#setItems should be ordered by key level accuracy in ascending order
 * (i.e., less accurate ones are at the front of the list).
 * 2. Use JSON string to store the list of Item instead of embedded Avro schema because other part
 * of the system support single layer Avro record better
 */
public class DunsGuideBook extends BaseFabricEntity<DunsGuideBook> implements FabricEntity<DunsGuideBook> {

    private static final String SRC_DUNS_KEY = "Duns";
    private static final String ITEMS_KEY = "Items";

    private static final TypeReference<List<Item>> ITEMS_TYPE_REFERENCE = new TypeReference<List<Item>>() {};

    private String srcDuns;
    private List<Item> items;

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = getSchema(recordType);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(SRC_DUNS_KEY, srcDuns);
        String itemsStr = JsonUtils.serialize(items == null ? Collections.emptyList() : items);
        builder.set(ITEMS_KEY, itemsStr);
        return builder.build();
    }

    @Override
    public Schema getSchema(String recordType) {
        Preconditions.checkNotNull(recordType);
        return SchemaBuilder.record(replaceSpacialChars(recordType)).fields()
                /* source DUNS */
                .name(SRC_DUNS_KEY)
                .type(Schema.create(Schema.Type.STRING))
                .noDefault()
                .name(ITEMS_KEY)
                /* JSON string of DunsGuideBook.Item array */
                .type(Schema.create(Schema.Type.STRING))
                .noDefault()
                .endRecord();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DunsGuideBook fromFabricAvroRecord(GenericRecord record) {
        Preconditions.checkNotNull(record);
        setId(getString(record.get(SRC_DUNS_KEY)));
        String itemsStr = getString(record.get(ITEMS_KEY));
        if (itemsStr != null) {
            List<Item> items = JsonUtils.deserialize(itemsStr, ITEMS_TYPE_REFERENCE);
            setItems(items);
        } else {
            setItems(Collections.emptyList());
        }
        return this;
    }

    @Override
    public DunsGuideBook fromHdfsAvroRecord(GenericRecord record) {
        // hdfs should have the same format as data fabric
        return fromFabricAvroRecord(record);
    }

    @Override
    public String getId() {
        return srcDuns;
    }

    @Override
    public void setId(String id) {
        this.srcDuns = id;
    }

    public List<Item> getItems() {
        return items;
    }

    public void setItems(List<Item> items) {
        this.items = items;
    }

    private String getString(Object avroValue) {
        if (avroValue instanceof Utf8) {
            return ((Utf8) avroValue).toString();
        } else {
            // NOTE: this can be null
            return (String) avroValue;
        }
    }

    private String replaceSpacialChars(@NotNull String recordType) {
        // we need to replace special char '.' from recordType otherwise avro
        // schema parser will run into exception
        return recordType.replace('.', '_');
    }

    /*
     * Holder object that contains the target duns to redirect to
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Item {
        private static final String DUNS_KEY = "TargetDuns";
        private static final String KEYPARTITION_KEY = "KeyPartition";
        private static final String BOOKSOURCE_KEY = "BookSource";

        @JsonProperty(DUNS_KEY)
        private String duns;
        @JsonProperty(KEYPARTITION_KEY)
        private String keyPartition;
        @JsonProperty(BOOKSOURCE_KEY)
        private String bookSource;

        public Item(String duns, String keyPartition, String bookSource) {
            this.duns = duns;
            this.keyPartition = keyPartition;
            this.bookSource = bookSource;
        }

        public String getDuns() {
            return duns;
        }

        public void setDuns(String duns) {
            this.duns = duns;
        }

        public String getKeyPartition() {
            return keyPartition;
        }

        public void setKeyPartition(String keyPartition) {
            this.keyPartition = keyPartition;
        }

        public String getBookSource() {
            return bookSource;
        }

        public void setBookSource(String bookSource) {
            this.bookSource = bookSource;
        }
    }
}
