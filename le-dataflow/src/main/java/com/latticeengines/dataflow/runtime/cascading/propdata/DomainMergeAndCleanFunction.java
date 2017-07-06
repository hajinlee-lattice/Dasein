package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.DomainUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DomainMergeAndCleanFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -1474963150979757098L;
    private List<String> domainNames;

    public DomainMergeAndCleanFunction(List<String> domainNames, String parsedDomain) {
        super(new Fields(parsedDomain));
        this.domainNames = domainNames;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        for (String domainName : domainNames) {
            try {
                String url = arguments.getString(domainName);
                if (StringUtils.isBlank(url)) {
                    continue;
                }
                String parsedDomain = DomainUtils.parseDomain(url.toLowerCase());
                if (StringUtils.isNotBlank(parsedDomain)) {
                    functionCall.getOutputCollector().add(new Tuple(parsedDomain));
                    return;
                }
            } catch (Exception e) {
            }
        }
        functionCall.getOutputCollector().add(Tuple.size(1));
    }
}
