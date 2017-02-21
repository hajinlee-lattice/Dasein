package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;

import com.latticeengines.common.exposed.query.ColumnLookup;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.dataflow.exposed.builder.strategy.AddFieldStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.AddColumnWithFixedValueStrategy;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public class SortOperation extends Operation {
    public SortOperation(Input prior, String field, boolean descending) {
        List<ColumnLookup> lookups = new ArrayList<>();
        ColumnLookup lookup = new ColumnLookup(field);
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
        List<String> originalFields = new ArrayList<>();
        for (FieldMetadata fm : prior.metadata) {
            originalFields.add(fm.getFieldName());
        }
        String randomColumnName = "SortTmp" + UUID.randomUUID().toString().replace("-", "");
        String randomColumnValue = UUID.randomUUID().toString().replace("-", "");
        AddFieldStrategy addFieldStrategy = new AddColumnWithFixedValueStrategy(randomColumnName, randomColumnValue,
                String.class);
        AddFieldOperation addFieldOperation = new AddFieldOperation(prior, addFieldStrategy);
        Pipe addDummyField = addFieldOperation.pipe;
        Pipe groupby = new GroupBy(addDummyField, new Fields(randomColumnName), getSortFields(sort),
                sort.getDescending());
        this.pipe = new Retain(groupby, DataFlowUtils.convertToFields(originalFields));
        this.metadata = new ArrayList<>(prior.metadata);
    }

    private Fields getSortFields(Sort sort) {
        List<String> fields = new ArrayList<>();
        for (ColumnLookup lookup : sort.getLookups()) {
            fields.add(lookup.getColumnName());
        }

        return new Fields(fields.toArray(new String[fields.size()]));
    }
}
