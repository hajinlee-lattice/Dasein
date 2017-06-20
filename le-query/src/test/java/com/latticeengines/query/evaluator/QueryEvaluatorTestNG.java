package com.latticeengines.query.evaluator;

import static com.latticeengines.query.evaluator.QueryTestUtils.getAttributeRepo;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.functionalframework.QueryFunctionalTestNGBase;

public class QueryEvaluatorTestNG extends QueryFunctionalTestNGBase {

    private AttributeRepository attrRepo;

    @BeforeTest(groups = "functional")
    public void setup() {
        attrRepo = getAttributeRepo();
    }

    @Test(groups = "functional")
    public void testAutowire() {
        Assert.assertNotNull(queryEvaluator);
    }

    @Test(groups = "functional")
    public void testLookup() {
        // simple lookup
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "companyname") //
                .select(BusinessEntity.Contact, "lastname") //
                .build();
        queryEvaluator.evaluate(attrRepo, query);

        // entity lookup
        query = Query.builder() //
                .find(BusinessEntity.Account) //
                .build();
        queryEvaluator.evaluate(attrRepo, query);

        // bucketed attribute
        query = Query.builder() //
                .select(BusinessEntity.LatticeAccount, "bucketed_attribute") //
                .select(BusinessEntity.Account, "companyname")
                .build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional")
    public void testRestriction() {
        // simple where clause
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "id").eq("59129793") //
                .build();
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "companyname", "city") //
                .where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // concrete on double
        restriction = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "alexaviewsperuser").eq(2.5) //
                .build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // column eqs column
        restriction = Restriction.builder() //
                .let(BusinessEntity.Account, "companyname").eq(BusinessEntity.Contact, "city") //
                .build();
        query = Query.builder().where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // range look up
        Restriction range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "companyname").in("a", "z") //
                .build();
        Restriction range2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "alexaviewsperuser").in(1.0, 3.5) //
                .build();
        restriction = Restriction.builder().and(range1, range2).build();
        query = Query.builder().where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query); //

        // half range look up
        range1 = Restriction.builder() //
                .let(BusinessEntity.Account, "companyname").gte("a") //
                .build();
        range2 = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "alexaviewsperuser").lt(3.5) //
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
                .let(BusinessEntity.LatticeAccount, "bucketed_attribute").eq("Label2") //
                .build();
        Restriction nullLbl = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "bucketed_attribute").eq(null) //
                .build();
        restriction = Restriction.builder().or(lbl2, nullLbl).build();
        query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query);

    }

    @Test(groups = "functional", expectedExceptions = QueryEvaluationException.class)
    public void testNonExistAttribute() {
        Query query = Query.builder() //
                .select(BusinessEntity.Account, "companyname", "street1") //
                .build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional", expectedExceptions = QueryEvaluationException.class)
    public void testNonExistBucket() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "bucketed_attribute").eq("blah blah") //
                .build();
        Query query = Query.builder().find(BusinessEntity.Account).where(restriction).build();
        queryEvaluator.evaluate(attrRepo, query);
    }

    @Test(groups = "functional")
    public void testSortAndPage() {
        Restriction nameIsCity = Restriction.builder() //
                .let(BusinessEntity.Account, "companyname").eq(BusinessEntity.Account, "city") //
                .build();
        Query query = Query.builder()
                .select(BusinessEntity.Account, "id", "companyname", "city") //
                .where(nameIsCity) //
                .orderBy(BusinessEntity.Account, "companyname") //
                .build();
        queryEvaluator.evaluate(attrRepo, query);
    }

}
