package com.latticeengines.dataflow.runtime.cascading.propdata;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.dataflow.runtime.cascading.BaseGroupbyBuffer;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

import cascading.operation.Buffer;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class BitEncodeBuffer extends BaseGroupbyBuffer implements Buffer, Serializable {

    private static final long serialVersionUID = -8024820880116725433L;
    private static final Log log = LogFactory.getLog(BitEncodeBuffer.class);

    private final BitCodeBook codeBook;
    private final String encodedField;
    private final String keyField;

    @SuppressWarnings("unused")
	private final String valueField;

    private final int encodeFieldPos;

    public BitEncodeBuffer(Fields fieldDeclaration, String keyField, String valueField, String encodedField,
            BitCodeBook codeBook) {
        super(fieldDeclaration);
        this.codeBook = codeBook;
        this.encodedField = encodedField;
        this.keyField = keyField;
        this.valueField = valueField;
        this.encodeFieldPos = namePositionMap.get(encodedField.toLowerCase());
    }

    protected void setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        List<Integer> trueBits = new ArrayList<>();
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            trueBits.addAll(trueBits(arguments, codeBook));
        }
        try {
            int[] trueBitsArray = ArrayUtils.toPrimitive(trueBits.toArray(new Integer[trueBits.size()]));
            String value = BitCodecUtils.encode(trueBitsArray);
            result.set(encodeFieldPos, value);
        } catch (IOException e) {
            log.error("Failed to encode " + encodedField, e);
        }
    }

    private List<Integer> trueBits(TupleEntry arguments, BitCodeBook codeBook) {
        switch (codeBook.getAlgorithm()) {
        case KEY_EXISTS:
            return encodeKeyExists(arguments, codeBook);
        default:
            return emptyList();
        }
    }

    private List<Integer> encodeKeyExists(TupleEntry arguments, BitCodeBook codeBook) {
        String key = arguments.getString(keyField);
        if (codeBook.hasKey(key)) {
            return singletonList(codeBook.getBitPosForkey(key));
        }
        return emptyList();
    }

}
