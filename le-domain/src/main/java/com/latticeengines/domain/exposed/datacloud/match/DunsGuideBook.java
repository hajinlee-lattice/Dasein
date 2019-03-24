package com.latticeengines.domain.exposed.datacloud.match;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datafabric.BaseFabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;

/**
 * Used to redirect source duns to a target duns that is obtained by a more
 * accurate match key.
 *
 * 1. Use JSON string to store the list of Item instead of embedded Avro schema
 * because other part of the system support single layer Avro record better
 */
public class DunsGuideBook extends BaseFabricEntity<DunsGuideBook>
        implements FabricEntity<DunsGuideBook> {

    public static final String SRC_DUNS_KEY = "Duns";
    public static final String SRC_DU_DUNS_KEY = "DUDuns"; // Serve CDL match
    public static final String SRC_GU_DUNS_KEY = "GUDuns"; // Serve CDL match
    public static final String ITEMS_KEY = "Items";
    // name of the tag used to check whether this entry is patched
    // the tag value will NOT be serialized to avro
    private static final String PATCHED_TAG = "Patched";

    private static final Schema OPTIONAL_STRING_SCHEMA = Schema.createUnion(
            Arrays.asList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)));
    private static final TypeReference<List<Item>> ITEMS_TYPE_REFERENCE = new TypeReference<List<Item>>() {
    };

    private String srcDuns;
    private String srcDUDuns;
    private String srcGUDuns;
    private List<Item> items;

    @Override
    public GenericRecord toFabricAvroRecord(String recordType) {
        Schema schema = getSchema(recordType);
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(SRC_DUNS_KEY, srcDuns);
        builder.set(SRC_DU_DUNS_KEY, srcDUDuns);
        builder.set(SRC_GU_DUNS_KEY, srcGUDuns);
        String itemsStr = JsonUtils.serialize(items == null ? Collections.emptyList() : items);
        builder.set(ITEMS_KEY, itemsStr);
        return builder.build();
    }

    @Override
    public Schema getSchema(String recordType) {
        Preconditions.checkNotNull(recordType);
        return SchemaBuilder.record(replaceSpacialChars(recordType)).fields()
                /* source DUNS */
                .name(SRC_DUNS_KEY).type(Schema.create(Schema.Type.STRING)).noDefault()
                /* source DU DUNS */
                .name(SRC_DU_DUNS_KEY).type(OPTIONAL_STRING_SCHEMA).noDefault()
                /* source GU DUNS */
                .name(SRC_GU_DUNS_KEY).type(OPTIONAL_STRING_SCHEMA).noDefault()
                /* JSON string of DunsGuideBook.Item array */
                .name(ITEMS_KEY).type(Schema.create(Schema.Type.STRING)).noDefault().endRecord();
    }

    @Override
    public DunsGuideBook fromFabricAvroRecord(GenericRecord record) {
        Preconditions.checkNotNull(record);
        setId(getString(record.get(SRC_DUNS_KEY)));
        setSrcDUDuns(getString(record.get(SRC_DU_DUNS_KEY)));
        setSrcGUDuns(getString(record.get(SRC_GU_DUNS_KEY)));
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

    public String getSrcDUDuns() {
        return srcDUDuns;
    }

    public void setSrcDUDuns(String srcDUDuns) {
        this.srcDUDuns = srcDUDuns;
    }

    public String getSrcGUDuns() {
        return srcGUDuns;
    }

    public void setSrcGUDuns(String srcGUDuns) {
        this.srcGUDuns = srcGUDuns;
    }

    public List<Item> getItems() {
        return items;
    }

    public void setItems(List<Item> items) {
        this.items = items;
    }

    public boolean isPatched() {
        return Boolean.TRUE.equals(getTag(PATCHED_TAG, Boolean.class));
    }

    public void setPatched(Boolean patched) {
        setTag(PATCHED_TAG, patched);
    }

    private String getString(Object avroValue) {
        if (avroValue instanceof Utf8) {
            return ((Utf8) avroValue).toString();
        } else {
            // NOTE: this can be null
            return (String) avroValue;
        }
    }

    /*
     * Holder object that contains the target duns to redirect to
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Item {
        public static final String DUNS_KEY = "TargetDuns";
        // Serve CDL match
        public static final String DU_DUNS_KEY = "TargetDUDuns";
        // Serve CDL match
        public static final String GU_DUNS_KEY = "TargetGUDuns";
        public static final String KEYPARTITION_KEY = "KeyPartition";
        public static final String BOOKSOURCE_KEY = "BookSource";
        private static final String PATCHED_KEY = "Patched";

        @JsonProperty(DUNS_KEY)
        private String duns;
        @JsonProperty(DU_DUNS_KEY)
        private String duDuns;
        @JsonProperty(GU_DUNS_KEY)
        private String guDuns;
        @JsonProperty(KEYPARTITION_KEY)
        private String keyPartition;
        @JsonProperty(BOOKSOURCE_KEY)
        private String bookSource;
        @JsonProperty(PATCHED_KEY)
        private Boolean patched = false;

        public Item(String duns, String duDuns, String guDuns, String keyPartition, String bookSource) {
            this.duns = duns;
            this.duDuns = duDuns;
            this.guDuns = guDuns;
            this.keyPartition = keyPartition;
            this.bookSource = bookSource;
        }

        public Item() {

        }

        public String getDuns() {
            return duns;
        }

        public void setDuns(String duns) {
            this.duns = duns;
        }

        public String getDuDuns() {
            return duDuns;
        }

        public void setDuDuns(String duDuns) {
            this.duDuns = duDuns;
        }

        public String getGuDuns() {
            return guDuns;
        }

        public void setGuDuns(String guDuns) {
            this.guDuns = guDuns;
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

        public Boolean getPatched() {
            return patched;
        }

        public void setPatched(Boolean patched) {
            this.patched = patched;
        }
    }
}
