package com.latticeengines.query.evaluator;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Cardinality;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRelationship;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRange;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.ExistsRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
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
    public void testConcreteRestrictionAgainstDouble() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setOperator(LogicalOperator.AND);
        restriction.addRestriction(new ConcreteRestriction(false, new ColumnLookup(SchemaInterpretation.Account,
                "alexaviewsperuser"), ComparisonType.EQUAL, new ValueLookup(2.5)));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        long count = queryEvaluator.evaluate(collection, query).fetchCount();
        assertEquals(count, 4858);
    }

    @Test(groups = "functional")
    public void testCompareColumnToOtherColumn() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setOperator(LogicalOperator.AND);
        restriction.addRestriction(new ConcreteRestriction(false, new ColumnLookup(SchemaInterpretation.Account,
                "companyname"), ComparisonType.EQUAL, new ColumnLookup(SchemaInterpretation.Account, "city")));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        long count = queryEvaluator.evaluate(collection, query).fetchCount();
        assertEquals(count, 211);
    }

    @Test(groups = "functional")
    public void testObjectTypeInRestrictionOptional() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setOperator(LogicalOperator.AND);
        restriction.addRestriction(new ConcreteRestriction(false, new ColumnLookup("companyname"),
                ComparisonType.EQUAL, new ColumnLookup("city")));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        long count = queryEvaluator.evaluate(collection, query).fetchCount();
        assertEquals(count, 211);
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
        List<Map<String, Object>> results = queryEvaluator.getResults(collection, query).getData();
        assertEquals(results.size(), 5);
        for (Map<String, Object> row : results) {
            assertEquals(2, row.size());
        }
    }

    @Test(groups = "functional", expectedExceptions = Exception.class)
    public void testUnknownColumnLookup() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setOperator(LogicalOperator.AND);
        restriction.addRestriction(new ConcreteRestriction(false, new ColumnLookup(SchemaInterpretation.Account,
                "poopy_cupcakes"), ComparisonType.EQUAL, new ValueLookup(12345)));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        queryEvaluator.evaluate(collection, query).fetchCount();
    }

    @Test(groups = "functional")
    public void testRangeLookup() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setOperator(LogicalOperator.AND);
        restriction.addRestriction(new ConcreteRestriction(false, new ColumnLookup(SchemaInterpretation.Account,
                "companyname"), ComparisonType.IN_RANGE, new RangeLookup("a", "z")));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        long count = queryEvaluator.evaluate(collection, query).fetchCount();
    }

    @Test(groups = "functional")
    public void testExistsRestriction() {
        DataCollection collection = getDataCollection();
        ExistsRestriction exists = new ExistsRestriction();
        exists.setObjectType(SchemaInterpretation.Contact);
        exists.setRestriction(new ConcreteRestriction(false, new ColumnLookup(SchemaInterpretation.Account,
                "companyname"), ComparisonType.IN_RANGE, new RangeLookup("a", "z")));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(exists);
        long count = queryEvaluator.evaluate(collection, query).fetchCount();
        assertEquals(count, 77058);
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        DataCollection collection = getDataCollection();
        Restriction restriction = new ConcreteRestriction(false, new ColumnLookup("companyname"), ComparisonType.EQUAL,
                new ColumnLookup("city"));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        query.setLookups(SchemaInterpretation.Account, "companyname", "city");
        query.setPageFilter(new PageFilter(0, 100));
        Sort sort = new Sort();
        sort.setDescending(false);
        sort.setLookups(SchemaInterpretation.Account, "companyname");
        query.setSort(sort);
        List<Map<String, Object>> results = queryEvaluator.getResults(collection, query).getData();
        assertEquals(results.size(), 100);
        String lastName = null;
        for (Map<String, Object> result : results) {
            String name = result.get("companyname").toString();
            if (lastName != null) {
                assertTrue(lastName.compareTo(name) <= 0);
            }
            lastName = name;
        }
    }

    @Test(groups = "functional")
    public void testBucketRestriction() {
        DataCollection collection = getDataCollection();
        LogicalRestriction restriction = new LogicalRestriction();
        restriction.setOperator(LogicalOperator.AND);
        BucketRange bucket = BucketRange.value(1);
        restriction.addRestriction(new BucketRestriction(new ColumnLookup(SchemaInterpretation.Account,
                "number_of_family_members"), bucket));
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.setRestriction(restriction);
        long count = queryEvaluator.evaluate(collection, query).fetchCount();
        assertEquals(count, 5);
    }

    @Test(groups = "functional")
    public void testOneToOneRelationshipJoin() {
        DataCollection collection = getDataCollection();
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.addLookup(new ColumnLookup(SchemaInterpretation.BucketedAccountMaster, "number_of_family_members"));
        query.addLookup(new ColumnLookup(SchemaInterpretation.Account, "companyname"));
        query.setPageFilter(new PageFilter(0, 100));
        List<Map<String, Object>> results = queryEvaluator.getResults(collection, query).getData();
        for (Map<String, Object> result : results) {
            assertEquals(result.size(), 2);
        }
    }

    @Test(groups = "functional", expectedExceptions = RuntimeException.class)
    public void testUnableToFindObjectForJoin() {
        DataCollection collection = getDataCollection();
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.addLookup(new ColumnLookup(SchemaInterpretation.Category, "foo"));
        queryEvaluator.evaluate(collection, query);
    }

    private DataCollection getDataCollection() {
        DataCollection collection = new DataCollection();
        Table account = getTable("querytest_table", SchemaInterpretation.Account);
        Table accountMaster = getTable("querytest_table_dup", SchemaInterpretation.BucketedAccountMaster);
        Table contact = getTable("querytest_table_dup", SchemaInterpretation.Contact);

        TableRelationship relationship = new TableRelationship();
        relationship.setSourceTable(account);
        relationship.setTargetTableName("querytest_table_dup");
        relationship.setSourceCardinality(Cardinality.ONE);
        relationship.setTargetCardinality(Cardinality.MANY);
        relationship.setSourceAttributes(Collections.singletonList("id"));
        relationship.setTargetAttributes(Collections.singletonList("id"));
        account.addRelationship(relationship);

        relationship = new TableRelationship();
        relationship.setSourceTable(account);
        relationship.setTargetTableName("querytest_table_dup");
        relationship.setSourceCardinality(Cardinality.ONE);
        relationship.setTargetCardinality(Cardinality.ONE);
        relationship.setSourceAttributes(Collections.singletonList("id"));
        relationship.setTargetAttributes(Collections.singletonList("id"));
        account.addRelationship(relationship);

        collection.getTables().add(account);
        collection.getTables().add(accountMaster);
        collection.getTables().add(contact);
        return collection;
    }

    private Table getTable(String name, SchemaInterpretation schemaInterpretation) {
        Table table = new Table();
        table.setName(name);
        table.setInterpretation(schemaInterpretation.toString());
        Attribute companyName = new Attribute();
        companyName.setName("companyname");
        table.addAttribute(companyName);
        Attribute id = new Attribute();
        id.setName("id");
        table.addAttribute(id);
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
        Attribute alexaViewsPerUser = new Attribute();
        alexaViewsPerUser.setName("alexaviewsperuser");
        table.addAttribute(alexaViewsPerUser);
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(JdbcStorage.DatabaseName.REDSHIFT);
        storage.setTableNameInStorage(name);
        table.setStorageMechanism(storage);
        return table;
    }
}
