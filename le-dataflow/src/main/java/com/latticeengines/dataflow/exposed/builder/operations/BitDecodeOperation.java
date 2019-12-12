package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.latticeengines.dataflow.runtime.cascading.propdata.stats.BitDecodeFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.tuple.Fields;

public class BitDecodeOperation extends Operation {

    private final BitCodeBook.DecodeStrategy decodeStrategy;

    public BitDecodeOperation(Input prior, String encodedField, String[] decodeFields, BitCodeBook codeBook) {
        this.decodeStrategy = codeBook.getDecodeStrategy();
        if (this.decodeStrategy == null) {
            throw new IllegalArgumentException("DecodeStrategy cannot be null for a bit decode opertion.");
        }
        this.metadata = constructMetadata(prior.metadata, decodeFields);
        Function<?> function = new BitDecodeFunction(new Fields(decodeFields), encodedField, decodeFields, codeBook);
        this.pipe = new Each(prior.pipe, new Fields(encodedField), function, Fields.ALL);
    }

    private List<FieldMetadata> constructMetadata(List<FieldMetadata> originalMetadataList, String[] decodeFields) {
        Class<?> outputClass = null;
        switch (this.decodeStrategy) {
        case BOOLEAN_YESNO:
            outputClass = String.class;
            break;
        default:
            throw new UnsupportedOperationException("Unsupported decode strategy " + decodeStrategy);
        }

        Set<String> originalFieldNames = new HashSet<>();
        List<FieldMetadata> fieldMetadatas = new ArrayList<>();
        for (FieldMetadata field : originalMetadataList) {
            originalFieldNames.add(field.getFieldName());
            fieldMetadatas.add(new FieldMetadata(field.getFieldName(), field.getJavaType()));
        }

        for (String decodeField : decodeFields) {
            if (originalFieldNames.contains(decodeField)) {
                throw new RuntimeException("The decode field " + decodeField + " conflict with an exiting field");
            }
            fieldMetadatas.add(new FieldMetadata(decodeField, outputClass));
        }

        return fieldMetadatas;
    }
}
