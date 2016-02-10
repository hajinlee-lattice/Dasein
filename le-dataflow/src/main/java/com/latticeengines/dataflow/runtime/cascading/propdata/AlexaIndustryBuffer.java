package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AlexaIndustryBuffer extends BaseOperation implements Buffer {

    private static final long serialVersionUID = 1833467220255737504L;

    private static final String CATEGORIES = "Categories";
    public static final String BUSINESS_INDUSTRY = "BusinessIndustry";
    public static final String BUSINESS_INDUSTRY_2 = "BusinessIndustry2";

    protected Map<String, Integer> namePositionMap;
    private Map<String, String> indMap;
    private Map<String, String> ind2Map;

    private AlexaIndustryBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
        this.namePositionMap = getPositionMap(fieldDeclaration);
    }

    public AlexaIndustryBuffer(String domainField, Map<String, String> indMap, Map<String, String> ind2Map) {
        this(new Fields(domainField, BUSINESS_INDUSTRY, BUSINESS_INDUSTRY_2));
        this.indMap = indMap;
        this.ind2Map = ind2Map;
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
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
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
            } else {
                System.out.println("Warning: can not find field name=" + fieldName);
            }
        }
    }

    private void setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();

            if (StringUtils.isEmpty(arguments.getString(CATEGORIES))) {
                return;
            }

            Set<String> categories = new HashSet<>();
            for (String token : arguments.getString(CATEGORIES).split(",")) {
                categories.add(StringUtils.trim(token));
            }

            String indInResult = result.getString(namePositionMap.get(BUSINESS_INDUSTRY));
            if (StringUtils.isEmpty(indInResult)) {
                for (String category : categories) {
                    if (indMap.containsKey(category)) {
                        result.set(namePositionMap.get(BUSINESS_INDUSTRY), indMap.get(category));
                        break;
                    }
                }
            }

            String ind2InResult = result.getString(namePositionMap.get(BUSINESS_INDUSTRY_2));
            if (StringUtils.isEmpty(ind2InResult)) {
                for (String category : categories) {
                    if (ind2Map.containsKey(category)) {
                        result.set(namePositionMap.get(BUSINESS_INDUSTRY_2), ind2Map.get(category));
                        break;
                    }
                }
            }
        }
    }
}
