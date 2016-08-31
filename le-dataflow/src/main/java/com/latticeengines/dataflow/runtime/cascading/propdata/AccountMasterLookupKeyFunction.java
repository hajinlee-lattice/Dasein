package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.domain.exposed.propdata.match.AccountLookupEntry;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AccountMasterLookupKeyFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 5238703196548002861L;

    private String domainColumn;
    private String dunsColumn;

    public AccountMasterLookupKeyFunction(String keyColumn, String domainColumn, String dunsColumn) {
        super(new Fields(keyColumn));
        this.domainColumn = domainColumn;
        this.dunsColumn = dunsColumn;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (domainColumn != null && dunsColumn != null) {
            String domain = arguments.getString(domainColumn);
            String duns = arguments.getString(dunsColumn);
            functionCall.getOutputCollector().add(new Tuple(AccountLookupEntry.buildId(domain, duns)));
        } else if (domainColumn != null) {
            String domain = arguments.getString(domainColumn);
            functionCall.getOutputCollector().add(new Tuple(AccountLookupEntry.buildId(domain, null)));
        } else if (dunsColumn != null) {
            String duns = arguments.getString(dunsColumn);
            functionCall.getOutputCollector().add(new Tuple(AccountLookupEntry.buildId(null, duns)));
        }
    }
}
