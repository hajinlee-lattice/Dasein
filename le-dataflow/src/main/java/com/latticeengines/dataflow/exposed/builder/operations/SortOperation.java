package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.List;

import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.latticeengines.common.exposed.query.ReferenceInterpretation;
import com.latticeengines.common.exposed.query.SingleReferenceLookup;
import com.latticeengines.common.exposed.query.Sort;

public class SortOperation extends Operation {
    public SortOperation(Input prior, String field, boolean descending) {
        List<SingleReferenceLookup> lookups = new ArrayList<>();
        SingleReferenceLookup lookup = new SingleReferenceLookup(field, ReferenceInterpretation.COLUMN);
        lookups.add(lookup);
        Sort sort = new Sort(lookups);
        sort.setDescending(descending);

        init(prior, sort);
    }

    public SortOperation(Input prior, String field) {
        this(prior, field, false);
    }

    public SortOperation(Input prior, Sort sort) {
        init(prior, sort);
    }

    private void init(Input prior, Sort sort) {
        Pipe priorPipe = prior.pipe;

        Pipe groupby = new GroupBy(priorPipe, Fields.NONE, getSortFields(sort), sort.getDescending());

        this.pipe = groupby;
        this.metadata = prior.metadata;
    }

    private Fields getSortFields(Sort sort) {
        List<String> fields = new ArrayList<>();
        for (SingleReferenceLookup lookup : sort.getLookups()) {
            if (lookup.getInterpretation() != ReferenceInterpretation.COLUMN) {
                throw new UnsupportedOperationException("Sorts are not supported on anything but columns");
            }
            fields.add(lookup.getReference().toString());
        }

        return new Fields(fields.toArray(new String[fields.size()]));
    }
}
