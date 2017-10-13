package com.latticeengines.query.evaluator;

import static org.testng.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.core.QueryException;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;

public class QueryDSLTestNG extends QueryFunctionalTestNGBase {

    @Autowired
    @Qualifier("redshiftDataSource")
    private DataSource redshiftDataSource;

    @Test(groups = "functional")
    public void testWindowFunctionComparison() throws SQLException {
        SQLQueryFactory factory = factory();
        StringPath tablePath = Expressions.stringPath("query_test");
        StringPath idPath = Expressions.stringPath(tablePath, "accountid");
        StringPath innerPath = Expressions.stringPath("inner");
        long count = factory
                .query()
                .from(factory //
                        .select(SQLExpressions.max(idPath).over().partitionBy(idPath).orderBy(idPath) //
                                        .rows().between().unboundedPreceding().currentRow().as("max1"),
                                SQLExpressions.max(idPath).over().partitionBy(idPath).orderBy(idPath) //
                                        .rows().between().unboundedPreceding().currentRow().as("max2")) //
                        .from(tablePath).as("inner")) //
                .where(Expressions.stringPath(innerPath, "max1").eq("59129793")
                        .and(Expressions.stringPath(innerPath, "max2").eq("59129793"))) //
                .fetchCount();
        System.out.println(count);
    }

    @Test(groups = "functional")
    public void testCount() throws SQLException {
        SQLQueryFactory factory = factory();
        StringPath tablePath = Expressions.stringPath("query_test");
        try (PerformanceTimer timer = new PerformanceTimer("getCount")) {
            long count = factory.query().from(tablePath).fetchCount();
            Assert.assertTrue(count > 0);
        }
    }

    @Test(groups = "functional")
    public void testExists() {
        StringPath outerTable = Expressions.stringPath("query_test");
        StringPath innerTable = Expressions.stringPath("query_test");

        StringPath outerColumn = Expressions.stringPath(outerTable, "accountid");
        StringPath innerColumn = Expressions.stringPath(innerTable, "accountid");

        long count = factory().query().from(outerTable)
                .where(factory().from(innerTable).where(innerColumn.eq(outerColumn)).exists()).fetchCount();
        assertEquals(count, 100000);
    }

    @Test(groups = "functional")
    @SuppressWarnings("unchecked")
    public void testIntersect() throws SQLException {
        StringPath outerTable = Expressions.stringPath("bernard_txn_poc");
        StringPath innerTable = Expressions.stringPath("bernard_txn_poc");
        StringPath outerColumn1 = Expressions.stringPath(outerTable, "account_id");
        StringPath outerColumn2 = Expressions.stringPath(outerTable, "period_id");
        StringPath innerColumn1 = Expressions.stringPath(innerTable, "account_id");
        StringPath innerColumn2 = Expressions.stringPath(innerTable, "period_id");
        SQLQuery withQuery1 = factory().query().from(outerTable).select(outerColumn1, outerColumn2).where(outerColumn1.eq("6"));
        SQLQuery withQuery2 = factory().query().from(innerTable).select(innerColumn1, innerColumn2).where(innerColumn1.eq("2"));
        SQLQuery withQuery3 = factory().query().from(innerTable)
                .select(innerColumn1, innerColumn2).where(innerColumn2.eq("2").and(innerColumn1.eq("6")));
        StringPath withTable1 = Expressions.stringPath("T1");
        StringPath withTable2 = Expressions.stringPath("T2");
        StringPath withTable3 = Expressions.stringPath("T3");
        StringPath withTable4 = Expressions.stringPath("T4");
        StringPath col1 = Expressions.stringPath("account_id");
        StringPath col2 = Expressions.stringPath("period_id");
        SQLQuery unionQuery1 = factory().query().select(SQLExpressions.all).from(withTable1);
        SQLQuery unionQuery2 = factory().query().select(SQLExpressions.all).from(withTable2);
        SQLQuery unionQuery3 = factory().query().select(SQLExpressions.all).from(withTable3);
        SQLQuery withQuery4 = factory().query().select(SQLExpressions.all)
                .from(SQLExpressions.intersect(SQLExpressions.union(unionQuery1, unionQuery2), unionQuery3));
        SQLQuery testQuery = factory().query()
                .with(withTable1, col1, col2).as(withQuery1)
                .with(withTable2, col1, col2).as(withQuery2)
                .with(withTable3, col1, col2).as(withQuery3)
                .with(withTable4, col1, col2).as(withQuery4)
                .orderBy(col2.asc())
                .from(withTable4)
                .select(SQLExpressions.all);
        System.out.println("sql = " + testQuery);
        //Assert.assertEquals(1, testQuery.fetchCount());
        ResultSet results = testQuery.getResults();
        while (results.next()) {
            Assert.assertEquals(6, results.getLong("account_id"));
            Assert.assertEquals(2, results.getLong("period_id"));
        }
    }

    private SQLQueryFactory factory() {
        SQLTemplates templates = new PostgreSQLTemplates();
        Configuration configuration = new Configuration(templates);
        return new SQLQueryFactory(configuration, redshiftDataSource);
    }

    @Test(groups = "functional")
    public void testWindowFunctionBug() throws SQLException {
        SQLQueryFactory factory = factory();
        StringPath tablePath = Expressions.stringPath("query_test");
        StringPath columnPath = Expressions.stringPath(tablePath, "accountid");
        StringPath innerPath = Expressions.stringPath("inner");
        SQLQuery<?> query = factory
                .query()
                .select(SQLExpressions.count().as("count"))
                .from(factory //
                        .select(SQLExpressions.max(columnPath).over().partitionBy(columnPath).orderBy(columnPath)
                                .rows().between().currentRow().following(1).as("max1")) //
                        .from(tablePath).as("inner")) //
                .where(Expressions.stringPath(innerPath, "max1").eq("59129793"));
        query.setUseLiterals(true);
        ResultSet results = query.getResults();
        while (results.next()) {
            System.out.println(results.getLong("count"));
        }

        // SQL is buggy. SQL is:
        /*
         * select count(*) as count from (select max(eventtable.id) over
         * (partition by eventtable.id order by eventtable.id asc rows between
         * current row and following 1) as max1 from eventtable) as "inner"
         * where "inner".max1 = '123'
         */
        // but should be (note position of 1):
        /*
         * select count(*) as count from (select max(eventtable.id) over
         * (partition by eventtable.id order by eventtable.id asc rows between
         * current row and 1 following) as max1 from eventtable) as "inner"
         * where "inner".max1 = '123'
         */
        // Options are:
        // - Fix WindowFunctions in QueryDSL by doing a git pull request
        // - Add a correct WindowFunction builder that when build() is called
        // return a simpleTemplate
        //
        // 2017-06-16, we hacked a new WindowRows.java in our code base to make QueryDSL-4.1.4 work in test.
        // not sure if it works in runtime as well.
    }
}
