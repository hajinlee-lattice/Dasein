package com.latticeengines.app.util;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.FrontEndBucket;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.QueryTranslator;

import edu.emory.mathcs.backport.java.util.Collections;

public class QueryTranslatorUnitTestNG {

    @Test(groups = "unit")
    @SuppressWarnings("unchecked")
    public void testTranslate() {
        FrontEndQuery query = new FrontEndQuery();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setAll(Collections.singletonList(new BucketRestriction(new AttributeLookup(
                BusinessEntity.Account, "Some_Bucketed_Value"), FrontEndBucket.nullBkt())));
        frontEndRestriction.setAny(Collections.singletonList(new BucketRestriction(new AttributeLookup(
                BusinessEntity.Account, "Some_Other_Bucketed_Value"), FrontEndBucket.nullBkt())));
        query.setRestriction(frontEndRestriction);

        Query result = QueryTranslator.translate(query);
        assertTrue(result.getRestriction() instanceof LogicalRestriction);
        LogicalRestriction parent = (LogicalRestriction) result.getRestriction();
        assertEquals(parent.getRestrictions().size(), 2);
        assertTrue(parent.getRestrictions().stream()
                .anyMatch(r -> ((LogicalRestriction) r).getOperator().equals(LogicalOperator.AND)));
        assertTrue(parent.getRestrictions().stream()
                .anyMatch(r -> ((LogicalRestriction) r).getOperator().equals(LogicalOperator.OR)));
        assertTrue(parent.getRestrictions().stream()
                .anyMatch(r -> ((LogicalRestriction) r).getOperator().equals(LogicalOperator.AND)));
    }
}
