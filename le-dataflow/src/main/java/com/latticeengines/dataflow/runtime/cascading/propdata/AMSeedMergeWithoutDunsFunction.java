package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AMSeedMergeWithoutDunsFunction extends BaseOperation implements Function {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(AMSeedMergeWithoutDunsFunction.class);

    private static final long serialVersionUID = -4930114585520783490L;
    private Map<String, Integer> namePositionMap;
    private Map<String, String> attrsFromLe;

    private int amsDunsLoc;
    private int amsIsPrimaryDomainLoc;
    private int amsIsPrimaryLocationLoc;
    private int amsNumberOfLocationLoc;
    private int amsDomainSourceLoc;
    private String leDomainSourceCol;

    private AMSeedMergeWithoutDunsFunction(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public AMSeedMergeWithoutDunsFunction(Fields fieldDeclaration, Map<String, String> attrsFromLe,
            String amsDunsColumn, String amsIsPrimaryDomainColumn, String amsIsPrimaryLocationColumn,
            String amsNumberOfLocationColumn, String amsDomainSourceColumn, String leDomainSourceCol) {
        this(fieldDeclaration);
        this.attrsFromLe = attrsFromLe;
        this.amsDunsLoc = namePositionMap.get(amsDunsColumn);
        this.amsIsPrimaryDomainLoc = namePositionMap.get(amsIsPrimaryDomainColumn);
        this.amsIsPrimaryLocationLoc = namePositionMap.get(amsIsPrimaryLocationColumn);
        this.amsNumberOfLocationLoc = namePositionMap.get(amsNumberOfLocationColumn);
        this.amsDomainSourceLoc = namePositionMap.get(amsDomainSourceColumn);
        this.leDomainSourceCol = leDomainSourceCol;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        for (Map.Entry<String, String> entry : attrsFromLe.entrySet()) {
            if (entry.getValue() != null) {
                result.set(namePositionMap.get(entry.getKey()), arguments.getObject(entry.getValue()));
            } else {
                result.set(namePositionMap.get(entry.getKey()), null);
            }
        }
        result.set(amsDunsLoc, null);
        String domainSource = arguments.getString(leDomainSourceCol);
        result.set(amsDomainSourceLoc, domainSource);
        result.set(amsIsPrimaryDomainLoc, "Y");
        result.set(amsIsPrimaryLocationLoc, "Y");
        result.set(amsNumberOfLocationLoc, 1);
        functionCall.getOutputCollector().add(result);
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }
}
