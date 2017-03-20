package com.latticeengines.query.evaluator.lookup;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRange;
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
        ColumnLookup columnLookup = (ColumnLookup) secondaryLookup;
        if (columnLookup != null) {
            Attribute attribute = getAttribute(columnLookup);

            List<BucketRange> buckets = attribute.getBucketRangeList();
            if (buckets != null && buckets.size() > 0) {
                int bucketIdx = 0;
                while (bucketIdx < buckets.size()) {
                    BucketRange bucket = buckets.get(bucketIdx);

                    String lookupValue = Objects.toString(lookup.getValue());

                    if (lookup.getValue() == null && bucket.isNullOnly()) {
                        break;
                    } else if (lookupValue.equals(Objects.toString(bucket.getMin()))
                            && lookupValue.equals(Objects.toString(bucket.getMax()))) {
                        break;
                    }
                    bucketIdx++;
                }

                if (bucketIdx == buckets.size()) {
                    throw new RuntimeException(String.format(
                            "Could not locate bucket to resolve bucketed attribute %s for specified value %s",
                            attribute.getName(), lookup.getValue()));
                }

                return Collections.singletonList(Expressions.asComparable(Integer.toString(bucketIdx)));
            }
        }

        return Collections.singletonList(Expressions.asComparable(lookup.getValue().toString()));
    }
}
