package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AccountMasterLookupBuffer extends BaseOperation implements Buffer {

    protected Map<String, Integer> namePositionMap;
    private String latticeIdField;
    private String domainField;
    private String dunsField;
    private String globalDunsField;
    private String primaryLocationField;
    private int domainLoc;
    private int latticeIdLoc;

    private AccountMasterLookupBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public AccountMasterLookupBuffer(Fields fieldDeclaration, String latticeIdField, String domainField,
            String dunsField, String globalDunsField, String primaryLocationField) {
        this(fieldDeclaration);
        this.latticeIdField = latticeIdField;
        this.domainField = domainField;
        this.dunsField = dunsField;
        this.globalDunsField = globalDunsField;
        this.primaryLocationField = primaryLocationField;
        this.domainLoc = namePositionMap.get(domainField);
        this.latticeIdLoc = namePositionMap.get(latticeIdField);
    }

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());
        // TupleEntry group = bufferCall.getGroup();
        // setupTupleForGroup(result, group);
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(result, arguments);
        bufferCall.getOutputCollector().add(result);
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

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field : fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            }
        }
    }

    // The group is ordered by primaryLocation descendingly
    private void setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        String domain = null;
        Long latticeId = null;
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            String primaryLocation = arguments.getString(primaryLocationField);
            String globalDuns = arguments.getString(globalDunsField);
            String duns = arguments.getString(dunsField);
            if (StringUtils.isEmpty(duns)) {
                domain = arguments.getString(domainField);
                latticeId = arguments.getLong(latticeIdField);
                break;
            }
            if (primaryLocation != null && primaryLocation.equals("Y")) {
                domain = arguments.getString(domainField);
                latticeId = arguments.getLong(latticeIdField);
                if (duns.equals(globalDuns)) {
                    break;
                }
            } else {
                if (domain == null) {
                    domain = arguments.getString(domainField);
                    latticeId = arguments.getLong(latticeIdField);
                }
                break;
            }
        }
        result.set(domainLoc, domain);
        result.set(latticeIdLoc, latticeId);
    }

}
