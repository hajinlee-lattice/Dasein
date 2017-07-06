package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class BomboraWeeklyAggBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 7231196587104103720L;

    protected Map<String, Integer> namePositionMap;
    private String contentSourcesAggField;
    private String uniqueUsersAggField;
    private int contentSourcesLoc;
    private int uniqueUsersLoc;
    private Set<String> contectSourcesContent;
    private Set<String> uniqueUsersContent;

    private BomboraWeeklyAggBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public BomboraWeeklyAggBuffer(Fields fieldDeclaration, String contentSourcesField, String contentSourcesAggField,
            String uniqueUsersField, String uniqueUsersAggField) {
        this(fieldDeclaration);
        this.contentSourcesAggField = contentSourcesAggField;
        this.uniqueUsersAggField = uniqueUsersAggField;
        this.contentSourcesLoc = namePositionMap.get(contentSourcesField);
        this.uniqueUsersLoc = namePositionMap.get(uniqueUsersField);
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

    @SuppressWarnings("unchecked")
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        contectSourcesContent = new HashSet<String>();
        uniqueUsersContent = new HashSet<String>();
        Tuple result = Tuple.size(getFieldDeclaration().size());
        TupleEntry group = bufferCall.getGroup();
        setupTupleForGroup(result, group);
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        setupTupleForArgument(result, arguments);
        bufferCall.getOutputCollector().add(result);
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

    private void setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            if (!StringUtils.isEmpty(arguments.getString(contentSourcesAggField))) {
                contectSourcesContent.add(arguments.getString(contentSourcesAggField));
            }
            if (!StringUtils.isEmpty(arguments.getString(uniqueUsersAggField))) {
                uniqueUsersContent.add(arguments.getString(uniqueUsersAggField));
            }
        }
        result.set(contentSourcesLoc, contectSourcesContent.size());
        result.set(uniqueUsersLoc, uniqueUsersContent.size());
    }
}
