package com.latticeengines.objectapi.object;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.query.exposed.object.BusinessObject;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;

@Component("accountObject")
public class AccountObject extends BusinessObject {
    private static final Log log = LogFactory.getLog(AccountObject.class);

    @Override
    public SchemaInterpretation getObjectType() {
        // TODO TEMPORARY FOR M10
        return SchemaInterpretation.BucketedAccountMaster;
    }

    @Override
    public Predicate processFreeFormSearch(DataCollection dataCollection, String freeFormRestriction) {
        // Just check whether name LIKE '%freeFormRestriction%'
        Table table = dataCollection.getTable(getObjectType());
        Attribute attribute = table.getAttribute("name");
        if (attribute == null) {
            log.warn(String.format("Could not find name column %s for free form search", "name"));
            return Expressions.TRUE;
        }
        StringPath columnPath = QueryUtils.getColumnPath(table, attribute);
        return columnPath.contains(Expressions.stringPath(freeFormRestriction));
    }
}