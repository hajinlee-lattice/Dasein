package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class MatchIDGenerationFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -5824474654030831002L;
    private String parsedDomainFieldName = null;
    private String dunsFieldName = null;

    public MatchIDGenerationFunction(List<String> keyFields, String matchIdKey) {
        super(new Fields(matchIdKey));
        if (keyFields.size() > 0) {
            if (keyFields.get(0).equals("__PARSED_DOMAIN__")) {
                this.parsedDomainFieldName = keyFields.get(0);
            } else {
                this.dunsFieldName = keyFields.get(0);
            }
        }
        if (keyFields.size() > 1) {
            if (keyFields.get(1).equals("__PARSED_DOMAIN__")) {
                this.parsedDomainFieldName = keyFields.get(1);
            } else {
                this.dunsFieldName = keyFields.get(1);
            }
        }
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String parsedDomain = parsedDomainFieldName != null ? arguments.getString(parsedDomainFieldName) : "NULL";
        String duns = dunsFieldName != null ? arguments.getString(dunsFieldName) : "NULL";
        String matchId = AccountLookupEntry.buildId(parsedDomain, duns);
        Tuple tuple = new Tuple(matchId);
        functionCall.getOutputCollector().add(tuple);
    }
}
