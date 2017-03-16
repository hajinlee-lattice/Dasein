package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class MiniAMSeedMergeDomainDuns extends BaseOperation implements Function {
    private static final long serialVersionUID = -3241191315907621173L;
    private String domainColumn;
    private String dunsColumn;
    private String typeColumn;
    private String valueColumn;
    private final static String DOMAIN_TYPE = "Domain";
    private final static String DUNS_TYPE = "Duns";
    private Map<String, Integer> namePositionMap;

    public MiniAMSeedMergeDomainDuns(Fields fieldDeclaration, String domainColumn, String dunsColumn, String typeColumn,
            String valueColumn) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
        this.domainColumn = domainColumn;
        this.dunsColumn = dunsColumn;
        this.typeColumn = typeColumn;
        this.valueColumn = valueColumn;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        String domain = arguments.getString(domainColumn);
        String duns = arguments.getString(dunsColumn);
        // To avoid adding empty fields as Null in the result
        if (StringUtils.isNotEmpty(domain)) {
            result.set(namePositionMap.get(typeColumn), DOMAIN_TYPE);
            result.set(namePositionMap.get(valueColumn), domain);
            functionCall.getOutputCollector().add(result);
        }
        if (StringUtils.isNotEmpty(duns)) {
            result.set(namePositionMap.get(typeColumn), DUNS_TYPE);
            result.set(namePositionMap.get(valueColumn), duns);
            functionCall.getOutputCollector().add(result);
        }
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        // Adding output columns in the Map with an index position
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

}
