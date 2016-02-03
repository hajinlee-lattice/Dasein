package com.latticeengines.propdata.dataflow.pivot;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class HGDataBothBuffer extends BaseOperation implements Buffer {


    private static final long serialVersionUID = -6707858599535647248L;

    private static final String CATEGORY_1 = "HG_Category_1";
    private static final String CATEGORY_2 = "HG_Category_2";
    private static final String BOTH = "Both";
    private final String domainField;

    HGDataBothBuffer(String domainField) {
        super(new Fields(domainField, BOTH));
        this.domainField = domainField;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        TupleEntry group = bufferCall.getGroup();

        String domain = group.getString(domainField);

        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        List<String> categories = getCategoriesFromArguments(arguments);

        for (String cat: categories) {
            bufferCall.getOutputCollector().add(new Tuple(domain, cat));
        }
    }

    private List<String> getCategoriesFromArguments(Iterator<TupleEntry> argumentsInGroup) {
        List<String> toReturn = new ArrayList<>();
        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            String cat1 = arguments.getString(CATEGORY_1);
            String cat2 = arguments.getString(CATEGORY_2);
            toReturn.add(cat1);
            toReturn.add(cat2);
        }
        return toReturn;
    }


}
