package com.latticeengines.dataflow.runtime.cascading.propdata.am;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.StringStandardizationUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class LatticeIdFunction extends BaseOperation implements Function {
    private static final Logger log = LoggerFactory.getLogger(LatticeIdFunction.class);
    private static final long serialVersionUID = 6601729348287655784L;

    private String field;

    public LatticeIdFunction(Fields fieldDeclaration, String field) {
        super(fieldDeclaration);
        this.field = field;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        try {
            Object idObj = arguments.getObject(this.field);
            if (idObj == null) {
                throw new IllegalArgumentException("Found null LatticeId");
            }
            String latticeActId = StringStandardizationUtils
                    .getStandardizedOutputLatticeID(String.valueOf(idObj));
            if (StringUtils.isBlank(latticeActId)) {
                throw new IllegalArgumentException(
                        String.format("Found invalid LatticeId: %s", latticeActId));
            }
            functionCall.getOutputCollector().add(new Tuple(latticeActId));
        } catch (Exception e) {
            // throw new RuntimeException("Fail to standardize LatticeId", e);
            functionCall.getOutputCollector().add(new Tuple());
            log.warn("Fail to standardize LatticeId", e);
        }
    }

}
