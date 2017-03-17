package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;

public class ValueLookupResolver extends LookupResolver {
    private ValueLookup lookup;
    private Lookup secondaryLookup;

    public ValueLookupResolver(ValueLookup lookup, SchemaInterpretation rootObjectType, DataCollection dataCollection,
            Lookup secondaryLookup) {
        super(dataCollection, rootObjectType);
        this.lookup = lookup;
        this.secondaryLookup = secondaryLookup;
    }

    @Override
    public List<ComparableExpression<String>> resolve() {
        if (secondaryLookup != null) {
            ColumnLookup columnLookup = (ColumnLookup) secondaryLookup;
            if (columnLookup != null) {
                Attribute attribute = getAttribute(columnLookup);

                List<String> buckets = attribute.getBucketList();
                if (buckets != null && buckets.size() > 0) {
                    // TODO temporary implementation
                    int bucketIdx = 0;
                    while (bucketIdx < buckets.size()) {
                        String bucket = buckets.get(bucketIdx);

                        if (bucket != null && bucket.equals(lookup.getValue().toString())) {
                            break;
                        }
                    }
                    if (bucketIdx == buckets.size()) {
                        throw new RuntimeException(String.format(
                                "Could not locate bucket to resolve bucketed attribute %s for specified value %s",
                                attribute.getName(), lookup.getValue()));
                    }

                    return Collections.singletonList(Expressions.asComparable(Integer.toString(bucketIdx)));
                }
            }
        }

        return Collections.singletonList(Expressions.asComparable(lookup.getValue().toString()));
    }
}
