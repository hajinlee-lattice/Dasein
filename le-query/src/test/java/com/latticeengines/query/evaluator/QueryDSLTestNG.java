package com.latticeengines.query.evaluator;

import static org.testng.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;


@SuppressWarnings({"unchecked", "rawtypes"})
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
                                        .rows().between().preceding(3).preceding(1).as("max2")) //
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
    public void testIntersect() throws SQLException {
        StringPath outerTable = Expressions.stringPath("bernard_txn_poc");
        StringPath innerTable = Expressions.stringPath("bernard_txn_poc");
        StringPath outerColumn1 = Expressions.stringPath(outerTable, "account_id");
        StringPath outerColumn2 = Expressions.stringPath(outerTable, "period_id");
        StringPath innerColumn1 = Expressions.stringPath(innerTable, "account_id");
        StringPath innerColumn2 = Expressions.stringPath(innerTable, "period_id");
        SQLQuery<?> withQuery1 = factory().query().from(outerTable).select(outerColumn1, outerColumn2).where(outerColumn1.eq("6"));
        SQLQuery<?> withQuery2 = factory().query().from(innerTable).select(innerColumn1, innerColumn2).where(innerColumn1.eq("2"));
        SQLQuery<?> withQuery3 = factory().query().from(innerTable)
                .select(innerColumn1, innerColumn2).where(innerColumn2.eq("2").and(innerColumn1.eq("6")));
        SQLQuery withQuery5 = factory().query().from(innerTable).select(innerColumn1, innerColumn2)
                .where(innerColumn1.eq("3"));
        StringPath withTable1 = Expressions.stringPath("T1");
        StringPath withTable2 = Expressions.stringPath("T2");
        StringPath withTable3 = Expressions.stringPath("T3");
        StringPath withTable4 = Expressions.stringPath("T4");
        StringPath col1 = Expressions.stringPath("account_id");
        StringPath col2 = Expressions.stringPath("period_id");
        SQLQuery unionQuery1 = factory().query().select(SQLExpressions.all).from(withTable1);
        SQLQuery unionQuery2 = factory().query().select(SQLExpressions.all).from(withTable2);
        SQLQuery<?> unionQuery3 = factory().query().select(SQLExpressions.all).from(withTable3);
        SQLQuery<?> withQuery4 = factory().query().select(SQLExpressions.all)
                .from(SQLExpressions.intersect(
                        SQLExpressions.except(SQLExpressions.union(unionQuery1, unionQuery2), withQuery5),
                        unionQuery3));
        SQLQuery<?> testQuery = factory().query()
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
    public void testHasEngagedOneLegBehind() throws SQLException {
        /*
        select accountid, periodid
        from   (select  keys.accountid,
                        keys.periodid,
                        trxn.val,
                        max (case when trxn.val >= 0 then 1 else 0 end)
                        over (partition by keys.accountid
                              order by keys.periodid
                              rows between 1 following and 1 following) as agg
                from    keys
                left join (select  accountid, periodid, totalamount as val
                           from    tftest_4_transaction_2017_10_31_19_44_08_utc
                           where   productid in ('3872223C9BA06C649D68E415E23A9446') ) as trxn
                on  keys.accountid = trxn.accountid and  keys.periodid = trxn.periodid) as aps
         where  agg = 1;
         */
        SQLQueryFactory factory = factory();
        int followingOffset = 1;
        String txTableName = "query_test_period_transaction";
        String productIdStr = "A78DF03BAC196BE9A08508FFDB433A31";
        String aggrAmountStr = "agg";
        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath accountId = Expressions.stringPath("accountid");
        StringPath periodId = Expressions.stringPath("periodid");
        StringPath productId = Expressions.stringPath("productId");
        StringPath totalAmount = Expressions.stringPath("totalamount");
        StringPath aggrAmount = Expressions.stringPath(aggrAmountStr);

        EntityPath<String> keysPath = new PathBuilder<>(String.class, "keys");
        EntityPath<String> trxnPath = new PathBuilder<>(String.class, "trxn");
        EntityPath<String> apsPath = new PathBuilder<>(String.class, "aps");
        StringPath keysAccountId = Expressions.stringPath(keysPath, "accountId");
        StringPath trxnAccountId = Expressions.stringPath(trxnPath, "accountId");
        StringPath keysPeriodId = Expressions.stringPath(keysPath, "periodId");
        StringPath trxnPeriodId = Expressions.stringPath(trxnPath, "periodId");
        StringPath trxnVal = Expressions.stringPath(trxnPath, "val");

        NumberExpression trxnValNumber = Expressions.numberPath(BigDecimal.class, trxnPath, "val");
        CaseBuilder caseBuilder = new CaseBuilder();
        NumberExpression trxnValExists = caseBuilder.when(trxnValNumber.goe(0)).then(1).otherwise(0);

        Expression windowAgg = SQLExpressions.max(trxnValExists).over().partitionBy(keysAccountId).orderBy(keysPeriodId)
                .rows().between().following(followingOffset).following(followingOffset).as(aggrAmount);

        SQLQuery<?> productQuery = factory.query().select(accountId, periodId, totalAmount.as("val")).from(tablePath)
                .where(productId.eq(productIdStr));

        SQLQuery<?> apsQuery = factory.query().select(keysAccountId, keysPeriodId, trxnVal, windowAgg)
                .from(keysPath).leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        SQLQuery<?> finalQuery = factory.query().select(accountId, periodId).from(apsQuery, apsPath)
                .where(aggrAmount.eq(String.valueOf(1)));

        System.out.println("finalQuery = " + finalQuery);
    }

    @Test(groups = "functional")
    public void testHasEngagedPrior() throws SQLException {
        SQLQueryFactory factory = factory();
        /*
        select accountid, periodid
        from   (select  keys.accountid,
                        keys.periodid,
                        trxn.val,
                        max (case when trxn.val >= 0 then 1 else 0 end)
                           over (partition by keys.accountid
                                 order by keys.periodid
                                 rows between unbounded preceding and 5 preceding) as agg
                from    keys
                left join (select  accountid,
                                   periodid,
                                   totalamount as val
                           from    tftest_4_transaction_2017_10_31_19_44_08_utc
                           where   productid = 'A78DF03BAC196BE9A08508FFDB433A31') as trxn
                 on  keys.accountid = trxn.accountid
                     and  keys.periodid = trxn.periodid) as aps
         where  agg = 1
         */

        int priorOffset = 5;
        String txTableName = "query_test_period_transaction";
        String productIdStr = "A78DF03BAC196BE9A08508FFDB433A31";
        String aggrValStr = "agg";
        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath accountId = Expressions.stringPath("accountid");
        StringPath periodId = Expressions.stringPath("periodid");
        StringPath productId = Expressions.stringPath("productId");
        StringPath totalAmount = Expressions.stringPath("totalamount");
        StringPath aggrVal = Expressions.stringPath(aggrValStr);

        EntityPath<String> keysPath = new PathBuilder<>(String.class, "keys");
        EntityPath<String> trxnPath = new PathBuilder<>(String.class, "trxn");
        EntityPath<String> apsPath = new PathBuilder<>(String.class, "aps");
        StringPath keysAccountId = Expressions.stringPath(keysPath, "accountId");
        StringPath trxnAccountId = Expressions.stringPath(trxnPath, "accountId");
        StringPath keysPeriodId = Expressions.stringPath(keysPath, "periodId");
        StringPath trxnPeriodId = Expressions.stringPath(trxnPath, "periodId");
        StringPath trxnVal = Expressions.stringPath(trxnPath, "val");

        NumberExpression trxnValNumber = Expressions.numberPath(BigDecimal.class, trxnPath, "val");
        // change for ever, at_least_once, and each
        CaseBuilder caseBuilder = new CaseBuilder();
        NumberExpression trxnValExists = caseBuilder.when(trxnValNumber.goe(0)).then(1).otherwise(0);

        // at_least_once and each
        //NumberExpression trxnValExists = caseBuilder.when(trxnValNumber.gt(?).then(1).otherwise(0);

        // change
        Expression windowAgg = SQLExpressions.max(trxnValExists).over().partitionBy(keysAccountId).orderBy(keysPeriodId)
                .rows().between().unboundedPreceding().preceding(priorOffset).as(aggrVal);
        // ever
        // Expression windowAgg = SQLExpressions.max(existsTrxnVal).over().partitionBy(keysAccountId).orderBy(keysPeriodId)
        //         .rows().between().unboundedPreceding().currentRow().as(aggrAmount);

        // at_least_once and each
        //Expression windowAgg = SQLExpressions.max(trxnValExists).over().partitionBy(keysAccountId).orderBy(keysPeriodId)
        //        .rows().between().preceding(startOffset).preceding(endOffset).as(aggrAmount);

        // change for spent or unit
        SQLQuery<?> productQuery = factory.query().select(accountId, periodId, totalAmount.as("val")).from(tablePath)
                .where(productId.eq(productIdStr));

        SQLQuery<?> apsQuery = factory.query().select(keysAccountId, keysPeriodId, trxnVal, windowAgg)
                .from(keysPath).leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        SQLQuery<?> finalQuery = factory.query().select(accountId, periodId).from(apsQuery, apsPath)
                .where(aggrVal.eq(String.valueOf(1)));    // change
        //        .where(aggrVal.gt(String.valueOf(0)));   // at_least_once
        //        .where(aggrVal.gt(String.valueOf(?)));   // total sum
        System.out.println("finalQuery = " + finalQuery);

    }

    @Test(groups = "functional")
    public void testSumWithin() throws SQLException {
        SQLQueryFactory factory = factory();
        /*
        select accountid, periodid
        from   (select  keys.accountid,
                keys.periodid,
                trxn.val,
                sum (nvl(trxn.val,0))
                     over (partition by keys.accountid
                           order by keys.periodid
                           rows between 10 preceding
                                    and 5 preceding) as agg
                from    keys
                left join (select  accountid,
                           periodid,
                           totalamount as val
                     from    tftest_4_transaction_2017_10_31_19_44_08_utc
                     where   productid in ('A78DF03BAC196BE9A08508FFDB433A31') ) as trxn
                on keys.accountid = trxn.accountid and  keys.periodid = trxn.periodid) as aps
          where  agg >= 5000.0
         */
        int startOffset = 10;
        int endOffset = 5;
        int amount = 5000;
        String txTableName = "query_test_period_transaction";
        String productIdStr = "A78DF03BAC196BE9A08508FFDB433A31";
        String aggrAmountStr = "agg";
        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath accountId = Expressions.stringPath("accountid");
        StringPath periodId = Expressions.stringPath("periodid");
        StringPath productId = Expressions.stringPath("productId");
        StringPath totalAmount = Expressions.stringPath("totalamount");
        StringPath aggrAmount = Expressions.stringPath(aggrAmountStr);

        EntityPath<String> keysPath = new PathBuilder<>(String.class, "keys");
        EntityPath<String> trxnPath = new PathBuilder<>(String.class, "trxn");
        EntityPath<String> apsPath = new PathBuilder<>(String.class, "aps");
        StringPath keysAccountId = Expressions.stringPath(keysPath, "accountId");
        StringPath trxnAccountId = Expressions.stringPath(trxnPath, "accountId");
        StringPath keysPeriodId = Expressions.stringPath(keysPath, "periodId");
        StringPath trxnPeriodId = Expressions.stringPath(trxnPath, "periodId");
        StringPath trxnVal = Expressions.stringPath(trxnPath, "val");
        NumberExpression trxnNumber = Expressions.numberPath(BigDecimal.class, trxnPath, "val").coalesce(BigDecimal.ZERO).asNumber();
        Expression sumWindowAgg = SQLExpressions.sum(trxnNumber).over().partitionBy(keysAccountId).orderBy(keysPeriodId)
                .rows().between().preceding(startOffset).preceding(endOffset).as(aggrAmount);

        SQLQuery<?> productQuery = factory.query().select(accountId, periodId, totalAmount.as("val")).from(tablePath)
                .where(productId.eq(productIdStr));

        SQLQuery<?> apsQuery = factory.query().select(keysAccountId, keysPeriodId, trxnVal, sumWindowAgg)
                .from(keysPath).leftJoin(productQuery, trxnPath)
                .on(keysAccountId.eq(trxnAccountId).and(keysPeriodId.eq(trxnPeriodId)));

        SQLQuery<?> finalQuery = factory.query().select(accountId, periodId).from(apsQuery, apsPath)
                .where(aggrAmount.gt(String.valueOf(amount)));
        System.out.println("finalQuery = " + finalQuery);
    }

    @Test(groups = "functional")
    public void testMaxPeriodId() throws SQLException {
        SQLQueryFactory factory = factory();
        /*
        select max(periodid) from 'tftest_4_transaction_2017_10_31_19_44_08_utc'
         */
        String txTableName = "query_test_period_transaction";
        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath periodId = Expressions.stringPath("periodid");

        SQLQuery<?> maxPeriodIdSubQuery = factory.query().from(tablePath).select(periodId.max());
        System.out.println(maxPeriodIdSubQuery.toString());
    }

    private static String getDateDiffTemplate(String p) {
        return String.format("DATEDIFF('%s', date(%s), current_date)", p, "transactiondate");
    }

    @Test(groups = "functional")
    public void testGetAllPeriods() throws SQLException {
        SQLQueryFactory factory = factory();
        String txTableName = "query_test_transaction";

        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath periodId = Expressions.stringPath("periodid");
        Expression<?> maxPeriodOffset =
                Expressions.numberTemplate(BigDecimal.class, getDateDiffTemplate(PeriodStrategy.Template.Quarter.name())).max().add(1);
        StringPath numberPath = Expressions.stringPath("number");
        StringPath allPeriods = Expressions.stringPath("allperiods");
        NumberPath number = Expressions.numberPath(BigDecimal.class, "n");
        NumberPath dummy = Expressions.numberPath(BigDecimal.class, "dummy");

        SQLQuery<?> numberQuery = factory.query()
                .select(SQLExpressions.rowNumber().over(), Expressions.constant(1))
                .from(tablePath);
        SQLQuery<?> maxPeriodQuery = factory().from(tablePath).select(maxPeriodOffset);
        SQLQuery<?> allPeriodsQuery = factory.query()
                .select((Expression<?>) number.subtract(1), Expressions.constant(1))
                .from(numberPath)
                .where(number.loe(maxPeriodQuery));

        // has to use a dummy column or querydsl will not generate the right sql
        SQLQuery<?> query = factory.query()
                .with(numberPath, number, dummy).as(numberQuery)
                .with(allPeriods, periodId, dummy).as(allPeriodsQuery)
                .select(SQLExpressions.all)
                .from(allPeriods);

        System.out.println(query.toString());

    }


    @Test(groups = "functional")
    public void testAggregateTransactionByPeriod() throws SQLException {
        SQLQueryFactory factory = factory();
        String txTableName = "query_test_transaction";
        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath accountId = Expressions.stringPath("accountid");
        StringPath periodId = Expressions.stringPath("periodid");
        StringPath productId = Expressions.stringPath("productId");
        StringPath totalAmount = Expressions.stringPath("totalAmount");
        StringPath totalQuantity = Expressions.stringPath("totalQuantity");
        Expression<?> totalAmountSum = Expressions.numberPath(BigDecimal.class, "totalAmount").sum();
        Expression<?> totalQuantitySum = Expressions.numberPath(BigDecimal.class, "totalQuantity").sum();
        Expression<?> periodOffset =
                Expressions.numberTemplate(BigDecimal.class, getDateDiffTemplate(PeriodStrategy.Template.Quarter.name()));
        StringPath trxnPeriodPath = Expressions.stringPath("trxnbyperiod");


        /*
         with trxnbyperiod(totalamount, totalquantity, productid, periodid, accountid) as (
                 select sum(totalamount),sum( totalquantity), productid, datediff(quarter, date(transactiondate), current_date) as periodid, accountid from query_test_transaction
                 group by productid, datediff(quarter, date(transactiondate), current_date), accountid)
         select * from trxnbyperiod
         */


        SQLQuery<?> trxnByPeriodSubQuery = factory.from(tablePath)
                .select(totalAmountSum, totalQuantitySum, periodOffset, productId, accountId)
                .groupBy(periodOffset, productId, accountId);

        SQLQuery<?> trxnSelectAll = factory.query()
                .with(trxnPeriodPath, totalAmount, totalQuantity, periodId, productId, accountId).as(trxnByPeriodSubQuery)
                .select(SQLExpressions.all)
                .from(trxnPeriodPath);

        System.out.println(trxnSelectAll.toString());

    }

    @Test(groups = "functional")
    public void testAllKeys() throws SQLException {
        SQLQueryFactory factory = factory();
        /*
        select  crossprod.accountid, periodid
        from   (select  accountid,
                         periodid
                from   (select  distinct accountid
                         from    tftest_4_transaction_2017_10_31_19_44_08_utc) as allaccounts,
                        (select  distinct periodid
                         from    tftest_4_transaction_2017_10_31_19_44_08_utc) as allperiods) as crossprod
        inner join (select  accountid,
                    min(periodid) as minpid
                    from    tftest_4_transaction_2017_10_31_19_44_08_utc
                    group by  accountid) as periodrange
        on  crossprod.accountid = periodrange.accountid
        where  crossprod.periodid >= periodrange.minpid - 2
         */
        String txTableName = "query_test_period_transaction";
        StringPath tablePath = Expressions.stringPath(txTableName);
        StringPath accountId = Expressions.stringPath("accountid");
        StringPath periodId = Expressions.stringPath("periodid");
        EntityPath<String> periodRange = new PathBuilder<>(String.class, "periodRange");
        NumberPath minPid = Expressions.numberPath(BigDecimal.class, periodRange, "minpid");
        StringPath periodAccountId = Expressions.stringPath(periodRange, "accountid");

        EntityPath<String> crossProd = new PathBuilder<>(String.class, "crossprod");
        StringPath crossAccountId = Expressions.stringPath(crossProd, "accountid");
        StringPath crossPeriodId = Expressions.stringPath(crossProd, "periodid");
        SQLQuery<?> crossProdQuery = factory.query().select(accountId, periodId).from(
                factory.selectDistinct(accountId).from(tablePath).as("allaccounts"),
                factory.selectDistinct(periodId).from(tablePath).as("allperiods"));

        SQLQuery<?> periodRangeSubQuery =
                factory.query().select(accountId, SQLExpressions.min(periodId).as("minpid")).from(tablePath).groupBy(accountId);
        SQLQuery<?> crossProdSubQuery = factory.query().from(crossProdQuery, crossProd)
                .innerJoin(periodRangeSubQuery, periodRange).on(periodAccountId.eq(crossAccountId))
                .where(crossPeriodId.goe(minPid.subtract(2)));
        SQLQuery<?> minusProdSubQuery = crossProdSubQuery.select(crossAccountId, periodId);
        System.out.println(minusProdSubQuery.toString());
        Assert.assertEquals(minusProdSubQuery.fetchCount(), 8529);
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
