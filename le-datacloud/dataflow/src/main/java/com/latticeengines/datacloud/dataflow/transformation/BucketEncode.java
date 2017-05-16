package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.BucketEncode.BEAN_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
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
        Node am = addSource(parameters.getBaseTables().get(0));

        // rename row id column
        // this is legacy code, just to rename LatticeId to LatticeAccountId
        if (StringUtils.isNotBlank(parameters.rowIdField) && StringUtils.isNotBlank(parameters.renameRowIdField)) {
            am = am.rename(new FieldList(parameters.rowIdField), new FieldList(parameters.renameRowIdField));
        }

        // handle exclude fields
        List<String> excludeFields = parameters.excludeAttrs;
        if (excludeFields != null) {
            excludeFields.retainAll(am.getFieldNames());
            if (!excludeFields.isEmpty()){
                am = am.discard(new FieldList(excludeFields));
                am = am.retain(new FieldList(am.getFieldNames()));
            }
        }

        // handle encoded fields
        Node encoded = processEncodedFields(am, parameters.encAttrs);
        List<String> discardFields = findDiscardFields(am.getFieldNames(), parameters.encAttrs);
        encoded = encoded.discard(new FieldList(discardFields));
        return encoded;
    }

    private List<String> findDiscardFields(List<String> originalFields, List<DCEncodedAttr> encAttrs) {
        List<String> discardFields = new ArrayList<>(originalFields);
        List<String> relayFields = findRelayFields(originalFields, encAttrs);
        relayFields.removeIf(s -> s.length() > 64);
        discardFields.removeAll(relayFields);
        return discardFields;
    }

    private List<String> findRelayFields(List<String> originalFields, List<DCEncodedAttr> encAttrs) {
        Set<String> encodedFields = new HashSet<>();
        for (DCEncodedAttr encAttr : encAttrs) {
            for (DCBucketedAttr bktAttr : encAttr.getBktAttrs()) {
                if (bktAttr.getDecodedStrategy() == null) {
                    encodedFields.add(bktAttr.getNominalAttr());
                } else {
                    encodedFields.add(bktAttr.getDecodedStrategy().getEncodedColumn());
                }
            }
        }
        List<String> fieldsToRelay = new ArrayList<>();
        for (String originalField : originalFields) {
            if (!encodedFields.contains(originalField)) {
                fieldsToRelay.add(originalField);
            }
        }
        return fieldsToRelay;
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
                    posMap.get(key).put(bktAttr.getNominalAttr(), decodeStrategy.getBitPosition());
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

}
