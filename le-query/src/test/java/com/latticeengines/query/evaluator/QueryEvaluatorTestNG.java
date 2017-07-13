package com.latticeengines.query.evaluator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;

public class QueryEvaluatorTestNG extends QueryFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testAutowire() {
        Assert.assertNotNull(queryEvaluator);
    }

    @Test(groups = "functional")
    public void testLookup() {
        // simple lookup
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "CompanyName") //
                .select(BusinessEntity.Contact, "LastName") //
                .build();
        queryEvaluator.evaluate(attrRepo, query);

        // entity lookup
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .build();
        queryEvaluator.evaluate(attrRepo, query);

        // bucketed attribute
        query = Query.builder() //
                .select(BusinessEntity.LatticeAccount, "Bucketed_Attribute") //
                .select(BusinessEntity.Account, "CompanyName").build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional")
    public void testRestriction() {
        // simple where clause
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "ID").eq("59129793") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "CompanyName", "City") //
                .where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // concrete on double
        restriction = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "AlexaViewsPerUser").eq(2.5) //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // column eqs column
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").eq(BusinessEntity.Contact, "City") //
                .build();
        query = Query.builder().where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // range look up
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").in("a", "z") //
                .build();
        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "AlexaViewsPerUser").in(1.0, 3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // half range look up
        range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").gte("a") //
                .build();
        range2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "AlexaViewsPerUser").lt(3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // exists
        restriction = Restriction.builder() //
                .exists(BusinessEntity.Contact) //
                .that(range1) //
                .build();
        query = Query.builder().where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // bucket
        Restriction lbl2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "Bucketed_Attribute").eq("Label2") //
                .build();
        Restriction nullLbl = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "Bucketed_Attribute").eq(null) //
                .build();
        restriction = Restriction.builder().or(lbl2, nullLbl).build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional")
    public void testNullRestriction() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").isNull() //
                .build();
        Query query = Query.builder().where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").isNotNull() //
                .build();
        query = Query.builder().where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //
    }

    @Test(groups = "functional")
    public void testFreeText() {
        // freetext
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "CompanyName") //
                .freeText("intel") //
                .freeTextAttributes(BusinessEntity.LatticeAccount, "LDC_Domain", "LDC_Name") //
                .build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional", expectedExceptions = QueryEvaluationException.class)
    public void testNonExistAttribute() {
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "CompanyName", "Street1") //
                .build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional", expectedExceptions = QueryEvaluationException.class)
    public void testNonExistBucket() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "Bucketed_Attribute").eq("blah blah") //
                .build();
        Query query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        Restriction nameIsCity = Restriction.builder() //
                .let(BusinessEntity.Account, "CompanyName").eq(BusinessEntity.Account, "City") //
                .build();
        Query query = Query.builder().select(BusinessEntity.Account, "ID", "CompanyName", "City") //
                .where(nameIsCity) //
                .orderBy(BusinessEntity.Account, "CompanyName") //
                .build();
        queryEvaluator.evaluate(attrRepo, query);
    }
}
