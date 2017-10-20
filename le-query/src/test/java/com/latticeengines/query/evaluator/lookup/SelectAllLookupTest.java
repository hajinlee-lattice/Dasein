package com.latticeengines.query.evaluator.lookup;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.SelectAllLookup;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.sql.SQLQuery;

public class SelectAllLookupTest extends QueryFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testSelectAllLookup() {
        SelectAllLookup selectAll = new SelectAllLookup("tableName");
        LookupResolverFactory resolverFactory = new LookupResolverFactory(attrRepo, queryProcessor);
        LookupResolver<SelectAllLookup> resolver = resolverFactory.getLookupResolver(SelectAllLookup.class);
        SQLQuery<?> query = resolver.resolveForFrom(selectAll);

        sqlContains(query, "select *");
        sqlContains(query, "from tableName");

    }
}
