package com.latticeengines.dataflow.runtime.cascading;

import java.beans.ConstructorProperties;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;

import com.latticeengines.common.exposed.util.Base64Utils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AddMD5Hash extends BaseOperation implements Function {

    private static final long serialVersionUID = -9005009537092431868L;

    private Set<String> excludeFields;
    private Integer truncateLen;
    private boolean compressed;

    @ConstructorProperties({ "fieldDeclaration" })
    public AddMD5Hash(Fields fieldDeclaration, Set<String> excludeFields, Integer truncateLen, boolean compressed) {
        super(0, fieldDeclaration);
        this.excludeFields = excludeFields == null ? new HashSet<>() : excludeFields;
        this.truncateLen = truncateLen;
        this.compressed = compressed;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        String data = "";
        TupleEntry entry = functionCall.getArguments();
        for (int i = 0; i < entry.getFields().size(); i++) {
            if (excludeFields.contains(entry.getFields().get(i).toString())) {
                continue;
            }
            if (i > 0) {
                data += "|";
            }
            Object tupleValue = entry.getTuple().getObject(i);

            if (tupleValue == null) {
                tupleValue = "<null>";
            }
            data += tupleValue.toString();
        }
        if (truncateLen != null) {
            data = data.substring(0, Math.min(data.length(), truncateLen));
        }
        if (compressed) {
            functionCall.getOutputCollector().add(new Tuple(Base64Utils.encodeBase64(DigestUtils.md5(data))));
        } else {
            functionCall.getOutputCollector().add(new Tuple(DigestUtils.md5Hex(data)));
        }

    }

}
