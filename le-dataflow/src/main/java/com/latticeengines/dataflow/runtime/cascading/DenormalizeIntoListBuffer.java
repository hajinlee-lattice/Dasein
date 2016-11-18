package com.latticeengines.dataflow.runtime.cascading;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DenormalizeIntoListBuffer extends BaseGroupbyBuffer {

    private static final long serialVersionUID = -8650598882742440958L;

    public DenormalizeIntoListBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        List<Map<String, Object>> list = new ArrayList<>();
        while (argumentsInGroup.hasNext()) {
            Map<String, Object> map = new HashMap<>();
            TupleEntry entry = argumentsInGroup.next();

            for (Comparable<Fields> f : entry.getFields()) {
                map.put(f.toString(), entry.getObject(f));

            }
            list.add(map);
        }
        result.set(namePositionMap.get("listfeature"), list);
    }

}
