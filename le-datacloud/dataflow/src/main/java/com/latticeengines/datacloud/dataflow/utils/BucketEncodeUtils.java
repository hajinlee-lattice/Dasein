package com.latticeengines.datacloud.dataflow.utils;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ATTRNAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_BKTALGO;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_DECSTRAT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_ENCATTR;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_LOWESTBIT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_NUMBITS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PROFILE_ATTR_SRCATTR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketAlgorithm;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DateBucket;
import com.latticeengines.domain.exposed.metadata.Extract;

public class BucketEncodeUtils {

    public static List<Pair<String, Class<?>>> profileCols() {
        return Arrays.asList( //
                Pair.of(PROFILE_ATTR_ATTRNAME, String.class), //
                Pair.of(PROFILE_ATTR_SRCATTR, String.class), //
                Pair.of(PROFILE_ATTR_DECSTRAT, String.class), //
                Pair.of(PROFILE_ATTR_ENCATTR, String.class), //
                Pair.of(PROFILE_ATTR_LOWESTBIT, Integer.class), //
                Pair.of(PROFILE_ATTR_NUMBITS, Integer.class), //
                Pair.of(PROFILE_ATTR_BKTALGO, String.class) //
        );
    }

    public static Schema profileSchema(String recordName) {
        Map<String, Class<?>> schemaMap = new HashMap<>();
        List<Pair<String, Class<?>>> columns = profileCols();
        for (int i = 0; i < columns.size(); i++) {
            schemaMap.put(columns.get(i).getKey(), columns.get(i).getValue());
        }
        return AvroUtils.constructSchema(recordName, schemaMap);
    }

    public static boolean isProfile(GenericRecord record) {
        List<Schema.Field> fields = record.getSchema().getFields();
        Set<String> attrNames = new HashSet<>();
        fields.forEach(field -> attrNames.add(field.name()));
        BucketEncodeUtils.profileCols().forEach(p -> attrNames.remove(p.getLeft()));
        return attrNames.isEmpty();
    }

    public static boolean isProfileNode(Node node) {
        for (Extract extract : node.getSourceSchema().getExtracts()) {
            Iterator<GenericRecord> recordIterator = AvroUtils.iterator(node.getHadoopConfig(), extract.getPath());
            if (recordIterator.hasNext()) {
                GenericRecord record = recordIterator.next();
                return BucketEncodeUtils.isProfile(record);
            }
        }
        return false;
    }

    // fields that are not encoded and need to be kept
    public static List<String> retainFields(List<GenericRecord> records) {
        List<String> retainFields = new ArrayList<>();
        records.forEach(record -> {
            if (record.get(PROFILE_ATTR_ENCATTR) == null) {
                String srcAttr = record.get(PROFILE_ATTR_SRCATTR).toString();
                retainFields.add(srcAttr);
            }
        });
        return retainFields;
    }

    // fields that need to be bucketed but not encoded
    public static Map<String, BucketAlgorithm> bucketFields(List<GenericRecord> records) {
        Map<String, BucketAlgorithm> bktFields = new HashMap<>();
        records.forEach(record -> {
            if (record.get(PROFILE_ATTR_ENCATTR) == null && record.get(PROFILE_ATTR_BKTALGO) != null) {
                // name in bucketed avro
                String attrName = record.get(PROFILE_ATTR_ATTRNAME).toString();
                String serializedAlgo = record.get(PROFILE_ATTR_BKTALGO).toString();
                BucketAlgorithm algo = JsonUtils.deserialize(serializedAlgo, BucketAlgorithm.class);
                bktFields.put(attrName, algo);
            }
        });
        return bktFields;
    }

    // srcAttr -> attrName
    public static Map<String, String> renameFields(List<GenericRecord> records) {
        Map<String, String> attrs = new HashMap<>();
        records.forEach(record -> {
            if (record.get(PROFILE_ATTR_ENCATTR) == null) {
                String srcAttr = record.get(PROFILE_ATTR_SRCATTR).toString();
                String tgtAttr = record.get(PROFILE_ATTR_ATTRNAME).toString();
                if (!tgtAttr.equals(srcAttr)) {
                    attrs.put(record.get(PROFILE_ATTR_SRCATTR).toString(),
                            record.get(PROFILE_ATTR_ATTRNAME).toString());
                }
            }
        });
        return attrs;
    }

    public static List<DCEncodedAttr> encodedAttrs(List<GenericRecord> records) {
        Map<String, DCEncodedAttr> encAttrMap = new HashMap<>();
        records.forEach(record -> {
            if (record.get(PROFILE_ATTR_ENCATTR) != null) {
                // encoded attr
                String encAttr = record.get(PROFILE_ATTR_ENCATTR).toString();
                if (!encAttrMap.containsKey(encAttr)) {
                    encAttrMap.put(encAttr, new DCEncodedAttr(encAttr));
                }
                DCBucketedAttr bktAttr = bucketedAttr(record);
                encAttrMap.get(encAttr).addBktAttr(bktAttr);
            }
        });
        List<DCEncodedAttr> encAttrs = new ArrayList<>(encAttrMap.values());
        encAttrs.sort(Comparator.comparing(DCEncodedAttr::getEncAttr));
        return encAttrs;
    }

    private static DCBucketedAttr bucketedAttr(GenericRecord record) {
        if (record.get(PROFILE_ATTR_ENCATTR) != null) {
            String attrName = record.get(PROFILE_ATTR_ATTRNAME).toString();
            String srcAttr = record.get(PROFILE_ATTR_SRCATTR).toString();
            int lowestBit = (int) record.get(PROFILE_ATTR_LOWESTBIT);
            int numBits = (int) record.get(PROFILE_ATTR_NUMBITS);
            DCBucketedAttr attr = new DCBucketedAttr(attrName, srcAttr, lowestBit, numBits);

            String serializedAlgo = record.get(PROFILE_ATTR_BKTALGO).toString();
            BucketAlgorithm algo = JsonUtils.deserialize(serializedAlgo, BucketAlgorithm.class);
            attr.setBucketAlgo(algo);

            if (record.get(PROFILE_ATTR_DECSTRAT) != null) {
                String serializedDecodeStrategy = record.get(PROFILE_ATTR_DECSTRAT).toString();
                if (StringUtils.isNotBlank(serializedDecodeStrategy)) {
                    BitDecodeStrategy decodeStrategy = JsonUtils.deserialize(serializedDecodeStrategy,
                            BitDecodeStrategy.class);
                    attr.setDecodedStrategy(decodeStrategy);
                }
            }
            return attr;
        }
        throw new IllegalArgumentException("This record cannot be parsed to a bucked attr: " + record);
    }

    // find attrs need to use max bkt as total count, instead of summing bkt counts
    public static List<String> overlapBktAttrs(List<GenericRecord> records) {
        List<String> attrs = new ArrayList<>();
        for (GenericRecord record: records) {
            if (record.get(PROFILE_ATTR_BKTALGO) != null) {
                BucketAlgorithm algorithm = JsonUtils.deserialize(record.get(PROFILE_ATTR_BKTALGO).toString(), //
                        BucketAlgorithm.class);
                if (algorithm instanceof DateBucket) {
                    attrs.add(record.get(PROFILE_ATTR_ATTRNAME).toString());
                }
            }
        }
        return attrs;
    }

}
