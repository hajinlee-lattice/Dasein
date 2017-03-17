package com.latticeengines.query.evaluator.lookup;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.querydsl.core.types.dsl.ComparableExpression;

public abstract class LookupResolver {
    protected DataCollection dataCollection;
    protected SchemaInterpretation rootObjectType;

    public LookupResolver(DataCollection dataCollection, SchemaInterpretation rootObjectType) {
        this.dataCollection = dataCollection;
        this.rootObjectType = rootObjectType;
    }

    public abstract List<ComparableExpression<String>> resolve();

    public Attribute getAttribute(ColumnLookup columnLookup) {
        Table table = getTable(columnLookup);

        Attribute attribute = table.getAttributes().stream()
                .filter(a -> a.getName() != null && a.getName().equals(columnLookup.getColumnName())) //
                .findFirst().orElse(null);
        if (attribute == null) {
            throw new RuntimeException(String.format("Could not find attribute with name %s in table %s",
                    columnLookup.getColumnName(), table.getName()));
        }
        return attribute;
    }

    public Table getTable(ColumnLookup columnLookup) {
        Table table = dataCollection.getTable(columnLookup.getObjectType());
        if (table == null) {
            throw new RuntimeException(String.format("Could not find table of type %s in data collection %s",
                    columnLookup.getObjectType(), dataCollection.getName()));
        }
        return table;
    }
}
