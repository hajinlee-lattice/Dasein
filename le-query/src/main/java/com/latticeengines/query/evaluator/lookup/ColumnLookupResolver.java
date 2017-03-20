package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRange;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class ColumnLookupResolver extends LookupResolver {
    private ColumnLookup lookup;

    public ColumnLookupResolver(ColumnLookup lookup, SchemaInterpretation rootObjectType, DataCollection dataCollection) {
        super(dataCollection, rootObjectType);
        this.lookup = lookup;
    }

    @Override
    public List<ComparableExpression<String>> resolve() {
        if (lookup.getObjectType() == null) {
            lookup.setObjectType(rootObjectType);
        }
        Table table = getTable(lookup);
        Attribute attribute = getAttribute(lookup);

        List<BucketRange> buckets = attribute.getBucketRangeList();
        if (buckets != null && buckets.size() > 0) {
            String physicalColumnName = attribute.getPhysicalName();
            Integer offset = attribute.getBitOffset();
            if (offset == null) {
                offset = 0;
            }
            Integer numBits = attribute.getNumOfBits();
            if (numBits == null) {
                // TODO warn
                return Collections.singletonList(QueryUtils.getColumnPath(table, attribute));
            }
            return Collections.singletonList(Expressions.stringTemplate("({0}>>{1})&{2}",
                    QueryUtils.getColumnPath(table, physicalColumnName), //
                    offset, //
                    1 << numBits));// 2^numBits
        } else {
            return Collections.singletonList(QueryUtils.getColumnPath(table, attribute));
        }
    }
}
