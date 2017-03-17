package com.latticeengines.query.evaluator;

import static org.testng.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.testng.annotations.Test;

import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
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
        StringPath tablePath = Expressions.stringPath("querytest_table");
        StringPath columnPath = Expressions.stringPath(tablePath, "id");
        StringPath innerPath = Expressions.stringPath("inner");
        long count = factory
                .query()
                .from(factory //
                        .select(SQLExpressions.max(columnPath).over().partitionBy(columnPath).orderBy(columnPath)
                                .rows().between().unboundedPreceding().currentRow().as("max1"),
                                SQLExpressions.max(columnPath).over().partitionBy(columnPath).orderBy(columnPath)
                                        .rows().between().unboundedPreceding().currentRow().as("max2")) //
                        .from(tablePath).as("inner")) //
                .where(Expressions.stringPath(innerPath, "max1").eq("123")
                        .and(Expressions.stringPath(innerPath, "max2").eq("123"))) //
                .fetchCount();
        System.out.println(count);
    }

    @Test(groups = "functional")
    public void testCount() throws SQLException {
        SQLQueryFactory factory = factory();
        StringPath tablePath = Expressions.stringPath("querytest_table");
        try (PerformanceTimer timer = new PerformanceTimer("getCount")) {
            long count = factory.query().from(tablePath).fetchCount();
        }
    }

    @Test(groups = "functional")
    public void testExists() {
        StringPath outerTable = Expressions.stringPath("querytest_table");
        StringPath innerTable = Expressions.stringPath("querytest_table");

        StringPath outerColumn = Expressions.stringPath(outerTable, "id");
        StringPath innerColumn = Expressions.stringPath(innerTable, "id");

        long count = factory().query().from(outerTable)
                .where(factory().from(innerTable).where(innerColumn.eq(outerColumn)).exists()).fetchCount();
        assertEquals(count, 611136);
    }

    private SQLQueryFactory factory() {
        SQLTemplates templates = new PostgreSQLTemplates();
        Configuration configuration = new Configuration(templates);
        return new SQLQueryFactory(configuration, redshiftDataSource);
    }

    @Test(groups = "functional", expectedExceptions = Exception.class)
    public void testWindowFunctionBug() throws SQLException {
        SQLQueryFactory factory = factory();
        StringPath tablePath = Expressions.stringPath("querytest_table");
        StringPath columnPath = Expressions.stringPath(tablePath, "id");
        StringPath innerPath = Expressions.stringPath("inner");
        SQLQuery<?> query = factory
                .query()
                .select(SQLExpressions.count().as("count"))
                .from(factory //
                        .select(SQLExpressions.max(columnPath).over().partitionBy(columnPath).orderBy(columnPath)
                                .rows().between().currentRow().following(1).as("max1")) //
                        .from(tablePath).as("inner")) //
                .where(Expressions.stringPath(innerPath, "max1").eq("123"));
        query.setUseLiterals(true);
        ResultSet results = query.getResults();
        while (results.next()) {
            System.out.println(results.getString("count"));
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
    }
}
