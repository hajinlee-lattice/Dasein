package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AMLookupKeyFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 5238703196548002861L;

    private String domainColumn;
    private String dunsColumn;
    private String countryColumn;
    private String stateColumn;
    private String zipCodeColumn;

    public AMLookupKeyFunction(String keyColumn, String domainColumn, String dunsColumn,
            String countryColumn, String stateColumn, String zipCodeColumn) {
        super(new Fields(keyColumn));
        this.domainColumn = domainColumn;
        this.dunsColumn = dunsColumn;
        this.countryColumn = countryColumn;
        this.stateColumn = stateColumn;
        this.zipCodeColumn = zipCodeColumn;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        if (domainColumn != null && zipCodeColumn != null && countryColumn != null) {
            String domain = arguments.getString(domainColumn);
            String zipCode = arguments.getString(zipCodeColumn);
            String country = arguments.getString(countryColumn);
            functionCall.getOutputCollector()
                    .add(new Tuple(AccountLookupEntry.buildIdWithLocation(domain, null, country, null, zipCode)));
        } else if (domainColumn != null && stateColumn != null && countryColumn != null) {
            String domain = arguments.getString(domainColumn);
            String state = arguments.getString(stateColumn);
            String country = arguments.getString(countryColumn);
            functionCall.getOutputCollector()
                    .add(new Tuple(AccountLookupEntry.buildIdWithLocation(domain, null, country, state, null)));
        } else if (domainColumn != null && countryColumn != null) {
            String domain = arguments.getString(domainColumn);
            String country = arguments.getString(countryColumn);
            functionCall.getOutputCollector()
                    .add(new Tuple(AccountLookupEntry.buildIdWithLocation(domain, null, country, null, null)));
        } else if (domainColumn != null && dunsColumn != null) {
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
