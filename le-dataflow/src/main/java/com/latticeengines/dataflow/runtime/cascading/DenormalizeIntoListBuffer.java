package com.latticeengines.dataflow.runtime.cascading;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DenormalizeIntoListBuffer extends BaseGroupbyBuffer {

    private static final long serialVersionUID = -8650598882742440958L;
    
    private String listFeatureName;

    public DenormalizeIntoListBuffer(Fields fieldDeclaration, String listFeatureName) {
        super(fieldDeclaration);
        this.listFeatureName = listFeatureName;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Tuple setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        List<Tuple> list = new ArrayList<>();
        while (argumentsInGroup.hasNext()) {
            
            TupleEntry entry = argumentsInGroup.next();

            int i = 0;
            Object[] o = new Object[entry.getFields().size()];
            Tuple tuple = new Tuple(o);
            for (Comparable<Fields> f : entry.getFields()) {
                tuple.set(i++, entry.getObject(f));

            }
            list.add(tuple);
        }
        result.set(namePositionMap.get(listFeatureName.toLowerCase()), list);
        return result;
    }

}
