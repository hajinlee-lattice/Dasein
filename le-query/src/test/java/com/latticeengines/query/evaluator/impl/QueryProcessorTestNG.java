package com.latticeengines.query.evaluator.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Connective;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;

public class QueryProcessorTestNG extends QueryFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testAutowire() {
        assertNotNull(queryEvaluator);
    }

    @Test(groups = "functional")
    public void testConcreteRestriction() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setConnective(Connective.AND);
        restriction.addRestriction(new ConcreteRestriction(false, new ColumnLookup(SchemaInterpretation.Account, "id"),
                ComparisonType.EQUAL, new ValueLookup("59129793")));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        long count = queryEvaluator.evaluate(collection, query).fetchCount();
        assertEquals(count, 5);
    }

    @Test(groups = "functional")
    public void testBucketRestriction() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setConnective(Connective.AND);
        restriction.addRestriction(new BucketRestriction(new ColumnLookup(SchemaInterpretation.Account,
                "number_of_family_members"), 1));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        long count = queryEvaluator.evaluate(collection, query).fetchCount();
        assertEquals(count, 21263);
    }

    private DataCollection getDataCollection() {
        DataCollection collection = new DataCollection();
        Table table = new Table();
        table.setName("querytest_table");
        table.setInterpretation(SchemaInterpretation.Account.toString());
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(JdbcStorage.DatabaseName.REDSHIFT);
        storage.setTableNameInStorage("querytest_table");
        table.setStorageMechanism(storage);
        collection.getTables().add(table);
        return collection;
    }
}
