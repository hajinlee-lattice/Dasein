package com.latticeengines.query.evaluator.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;

public class QueryEvaluatorTestNG extends QueryFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testAutowire() {
        assertNotNull(queryEvaluator);
    }

    @Test(groups = "functional")
    public void testConcreteRestriction() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setOperator(LogicalOperator.AND);
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
        restriction.setOperator(LogicalOperator.AND);
        restriction.addRestriction(new BucketRestriction(new ColumnLookup(SchemaInterpretation.Account,
                "number_of_family_members"), 1));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        long count = queryEvaluator.evaluate(collection, query).fetchCount();
        assertEquals(count, 21263);
    }

    @Test(groups = "functional")
    public void testSelectAllColumns() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setOperator(LogicalOperator.AND);
        restriction.addRestriction(new ConcreteRestriction(false, new ColumnLookup(SchemaInterpretation.Account, "id"),
                ComparisonType.EQUAL, new ValueLookup("59129793")));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        List<Map<String, Object>> results = queryEvaluator.getResults(collection, query);
        assertEquals(results.size(), 5);
        for (Map<String, Object> row : results) {
            assertEquals(row.size(), 5);
        }
    }

    @Test(groups = "functional")
    public void testSelectSomeColumns() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setOperator(LogicalOperator.AND);
        restriction.addRestriction(new ConcreteRestriction(false, new ColumnLookup(SchemaInterpretation.Account, "id"),
                ComparisonType.EQUAL, new ValueLookup("59129793")));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        query.setLookups(SchemaInterpretation.Account, "companyname", "city");
        List<Map<String, Object>> results = queryEvaluator.getResults(collection, query);
        assertEquals(results.size(), 5);
        for (Map<String, Object> row : results) {
            assertEquals(2, row.size());
        }
    }

    private DataCollection getDataCollection() {
        DataCollection collection = new DataCollection();
        Table table = new Table();
        table.setName("querytest_table");
        table.setInterpretation(SchemaInterpretation.Account.toString());
        Attribute companyName = new Attribute();
        companyName.setName("companyname");
        table.addAttribute(companyName);
        Attribute city = new Attribute();
        city.setName("city");
        table.addAttribute(city);
        Attribute state = new Attribute();
        state.setName("state");
        table.addAttribute(state);
        Attribute lastName = new Attribute();
        lastName.setName("lastname");
        table.addAttribute(lastName);
        Attribute familyMembers = new Attribute();
        familyMembers.setName("number_of_family_members");
        table.addAttribute(familyMembers);
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(JdbcStorage.DatabaseName.REDSHIFT);
        storage.setTableNameInStorage("querytest_table");
        table.setStorageMechanism(storage);
        collection.getTables().add(table);
        return collection;
    }
}
