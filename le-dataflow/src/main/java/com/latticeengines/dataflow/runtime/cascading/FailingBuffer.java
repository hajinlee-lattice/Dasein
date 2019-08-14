package com.latticeengines.dataflow.runtime.cascading;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class FailingBuffer extends BaseGroupbyBuffer {

    private static final long serialVersionUID = -8650598882542440958L;

    public FailingBuffer(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Tuple setupTupleForArgument(Tuple result, Iterator<TupleEntry> argumentsInGroup) {
        List<Tuple> list = new ArrayList<>();
        argumentsInGroup.forEachRemaining(tupleEntry -> {
            int i = 0;
            Object[] o = new Object[tupleEntry.getFields().size()];
            Tuple tuple = new Tuple(o);
            for (Comparable<Fields> f : tupleEntry.getFields()) {
                tuple.set(i++, tupleEntry.getObject(f));

            }
            list.add(tuple);
        });
        if (CollectionUtils.isNotEmpty(list)) {
            throw new RuntimeException("Mimic: Shuffle failed with too many fetch failures and insufficient progress");
        }
        return result;
    }

}
