package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.List;

import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

import com.latticeengines.common.exposed.query.ReferenceInterpretation;
import com.latticeengines.common.exposed.query.SingleReferenceLookup;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;

public class SortOperation extends Operation {
    public SortOperation(String prior, Sort sort, CascadingDataFlowBuilder builder) {
        super(builder);

        if (!builder.enforceGlobalOrdering()) {
            throw new RuntimeException("Builder must enforce global ordering in order to perform a sort operation");
        }

        Pipe priorPipe = getPipe(prior);

        Pipe groupby = new GroupBy(priorPipe, Fields.NONE, getSortFields(sort), sort.getDescending());

        this.pipe = groupby;
        this.metadata = getMetadata(prior);
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
