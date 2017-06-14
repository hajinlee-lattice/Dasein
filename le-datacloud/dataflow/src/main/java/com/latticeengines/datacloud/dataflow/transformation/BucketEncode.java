package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.BucketEncode.BEAN_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.dataflow.runtime.cascading.propdata.BucketEncodeFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.BitDecodeStrategy;
import com.latticeengines.domain.exposed.datacloud.dataflow.BucketEncodeParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCBucketedAttr;
import com.latticeengines.domain.exposed.datacloud.dataflow.DCEncodedAttr;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.operation.Function;

@Component(BEAN_NAME)
public class BucketEncode extends TypesafeDataFlowBuilder<BucketEncodeParameters> {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(BucketEncode.class);

    public static final String BEAN_NAME = "bucketEncode";

    @Override
    public Node construct(BucketEncodeParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(parameters.srcIdx));

        // handle exclude and rename fields
        List<String> toDiscard = new ArrayList<>(source.getFieldNames());
        toDiscard.removeAll(parameters.retainAttrs);
        List<String> fieldsNeededForEncode = fieldsNeededForEncode(parameters.encAttrs);
        toDiscard.removeAll(fieldsNeededForEncode);
        source = source.discard(new FieldList(toDiscard));

        List<String> oldFields = new ArrayList<>(parameters.renameFields.keySet());
        oldFields.retainAll(source.getFieldNames());
        List<String> newFields = new ArrayList<>();
        oldFields.forEach(f -> newFields.add(parameters.renameFields.get(f)));
        if (!oldFields.isEmpty()) {
            source = source.rename(new FieldList(oldFields), new FieldList(newFields));
        }

        // handle encoded fields
        Node encoded = processEncodedFields(source, parameters.encAttrs);
        Set<String> fieldsToKeep = new HashSet<>(parameters.retainAttrs);
        newFields.forEach(fieldsToKeep::add);
        fieldsNeededForEncode.removeAll(fieldsToKeep);
        return encoded.discard(new FieldList(fieldsNeededForEncode));
    }

    private Node processEncodedFields(Node am, List<DCEncodedAttr> encAttrs) {
        Node encoded = am.renamePipe("encoded");
        Map<String, BitCodeBook> codeBooks = bitCodeBookMap(encAttrs);
        Function<?> function = new BucketEncodeFunction(encAttrs, codeBooks);
        List<FieldMetadata> fms = encodedMetadata(encAttrs);
        List<String> outputFields = am.getFieldNames();
        outputFields.addAll(DataFlowUtils.getFieldNames(fms));
        return encoded.apply(function, new FieldList(am.getFieldNamesArray()), fms, new FieldList(outputFields));
    }

    private Map<String, BitCodeBook> bitCodeBookMap(List<DCEncodedAttr> encAttrs) {
        Map<String, BitCodeBook> map = new HashMap<>();
        Map<String, Map<String, Integer>> posMap = new HashMap<>();
        for (DCEncodedAttr encAttr : encAttrs) {
            for (DCBucketedAttr bktAttr: encAttr.getBktAttrs()) {
                BitDecodeStrategy decodeStrategy = bktAttr.getDecodedStrategy();
                if (decodeStrategy != null) {
                    String key = decodeStrategy.codeBookKey();
                    if (!map.containsKey(key)) {
                        BitCodeBook codeBook = new BitCodeBook(BitCodeBook.DecodeStrategy.valueOf(decodeStrategy.getBitInterpretation()));
                        codeBook.bindEncodedColumn(decodeStrategy.getEncodedColumn());
                        map.put(key, codeBook);
                    }
                    if (!posMap.containsKey(key)) {
                        posMap.put(key, new HashMap<>());
                    }
                    posMap.get(key).put(bktAttr.resolveSourceAttr(), decodeStrategy.getBitPosition());
                }
            }
        }
        for (Map.Entry<String, BitCodeBook> entry: map.entrySet()) {
            entry.getValue().setBitsPosMap(posMap.get(entry.getKey()));
        }
        return map;
    }

    private List<FieldMetadata> encodedMetadata(List<DCEncodedAttr> encAttrs) {
        List<FieldMetadata> fms = new ArrayList<>();
        encAttrs.forEach(encAttr -> fms.add(new FieldMetadata(encAttr.getEncAttr(), Long.class)));
        return fms;
    }

    private List<String> fieldsNeededForEncode(List<DCEncodedAttr> encAttrs) {
        Set<String> neededFields = new HashSet<>();
        for (DCEncodedAttr encAttr: encAttrs) {
            for (DCBucketedAttr bktAttr: encAttr.getBktAttrs()) {
                String srcAttr;
                if (bktAttr.getDecodedStrategy() != null) {
                    srcAttr = bktAttr.getDecodedStrategy().getEncodedColumn();
                } else {
                    srcAttr = bktAttr.resolveSourceAttr();
                }
                neededFields.add(srcAttr);
            }
        }
        return new ArrayList<>(neededFields);
    }

}
