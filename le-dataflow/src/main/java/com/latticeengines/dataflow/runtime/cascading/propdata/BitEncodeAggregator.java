package com.latticeengines.dataflow.runtime.cascading.propdata;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class BitEncodeAggregator extends BaseAggregator<BitEncodeAggregator.Context>
        implements Aggregator<BitEncodeAggregator.Context> {

    public static class Context extends BaseAggregator.Context {
        List<Integer> trueBits = new ArrayList<>();
    }

    private static final long serialVersionUID = -8024820880116725433L;
    private static final Log log = LogFactory.getLog(BitEncodeAggregator.class);

    private final BitCodeBook codeBook;
    private final String encodedField;
    private final String keyField;

    @SuppressWarnings("unused")
    private final String valueField;

    private final int encodeFieldPos;

    public BitEncodeAggregator(Fields fieldDeclaration, String keyField, String valueField, String encodedField,
            BitCodeBook codeBook) {
        super(fieldDeclaration);
        if (codeBook.getEncodeAlgo() == null) {
            throw new IllegalArgumentException("Cannot find encode algorithm in the code book.");
        }
        this.codeBook = codeBook;
        this.encodedField = encodedField;
        this.keyField = keyField;
        this.valueField = valueField;
        this.encodeFieldPos = namePositionMap.get(encodedField);
    }

    protected boolean isDummyGroup(TupleEntry group) {
        return false;
    }

    protected Context initializeContext(TupleEntry group) {
        return new Context();
    }

    protected Context updateContext(Context context, TupleEntry arguments) {
        context.trueBits.addAll(trueBits(arguments, codeBook));
        return context;
    }

    protected Tuple finalizeContext(Context context) {
        Tuple result = Tuple.size(fieldDeclaration.size());
        setupTupleForGroup(result, context.groupTuple);
        try {
            List<Integer> trueBits = context.trueBits;
            int[] trueBitsArray = ArrayUtils.toPrimitive(trueBits.toArray(new Integer[trueBits.size()]));
            String value = BitCodecUtils.encode(trueBitsArray);
            result.set(encodeFieldPos, value);
        } catch (IOException e) {
            log.error("Failed to encode " + encodedField, e);
        }
        return result;
    }

    private List<Integer> trueBits(TupleEntry arguments, BitCodeBook codeBook) {
        switch (codeBook.getEncodeAlgo()) {
        case KEY_EXISTS:
            return encodeKeyExists(arguments, codeBook);
        default:
            return emptyList();
        }
    }

    private List<Integer> encodeKeyExists(TupleEntry arguments, BitCodeBook codeBook) {
        String key = arguments.getString(keyField);
        if (codeBook.hasKey(key)) {
            return singletonList(codeBook.getBitPosForKey(key));
        }
        return emptyList();
    }

}
