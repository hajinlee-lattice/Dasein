package com.latticeengines.app.util;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.app.exposed.util.QueryTranslator;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.FlattenedRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import edu.emory.mathcs.backport.java.util.Collections;

public class QueryTranslatorUnitTestNG {

    @Test(groups = "unit")
    @SuppressWarnings("unchecked")
    public void testTranslate() {
        FrontEndQuery query = new FrontEndQuery();
        FlattenedRestriction flattenedRestriction = new FlattenedRestriction();
        flattenedRestriction.setAll(Collections.singletonList(new BucketRestriction(new ColumnLookup(
                "Some_Bucketed_Value"), 1)));
        flattenedRestriction.setAny(Collections.singletonList(new BucketRestriction(new ColumnLookup(
                "Some_Other_Bucketed_Value"), 2)));
        query.setRestriction(flattenedRestriction);

        QueryTranslator translator = new QueryTranslator(query, SchemaInterpretation.Account);
        Query result = translator.translate();
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
