package com.latticeengines.objectapi.object;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.query.exposed.object.BusinessObject;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;

@Component("accountObject")
public class AccountObject extends BusinessObject {
    @Override
    public SchemaInterpretation getObjectType() {
        return SchemaInterpretation.Account;
    }

    @Override
    public Predicate processFreeFormSearch(DataCollection dataCollection, String freeFormRestriction) {
        // Just check whether name LIKE '%freeFormRestriction%'
        StringPath table = Expressions.stringPath(getTableName(dataCollection));
        StringPath column = Expressions.stringPath(table, "name");
        return column.contains(Expressions.stringPath(freeFormRestriction));
    }
}