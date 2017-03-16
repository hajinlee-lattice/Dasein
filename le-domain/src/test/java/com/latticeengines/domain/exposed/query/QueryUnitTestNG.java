package com.latticeengines.domain.exposed.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class QueryUnitTestNG {

    /**
     * Simple test to assert that the Query interface will not change
     */
    @Test(groups = "unit")
    public void testStableInterface() {
        String serialized = "{\"lookups\":null,\"restriction\":{\"logicalRestriction\":{\"operator\":\"AND\",\"restrictions\":[{\"logicalRestriction\":{\"operator\":\"OR\",\"restrictions\":[],\"childMap\":null,\"children\":[]}},{\"logicalRestriction\":{\"operator\":\"AND\",\"restrictions\":[],\"childMap\":null,\"children\":[]}}],\"childMap\":null,\"children\":[{\"operator\":\"OR\",\"restrictions\":[],\"childMap\":null,\"children\":[]},{\"operator\":\"AND\",\"restrictions\":[],\"childMap\":null,\"children\":[]}]}},\"sort\":null,\"object_type\":\"Account\",\"page_filter\":null,\"free_form_text_search\":null}";
        Query query = JsonUtils.deserialize(serialized, Query.class);
        assertNotNull(query);
    }

    @Test(groups = "unit")
    public void testGetJoins() {
        Query query = new Query();
        query.setObjectType(SchemaInterpretation.Account);
        query.addLookup(new ColumnLookup(SchemaInterpretation.Account, "foo"));
        query.addLookup(new ColumnLookup(SchemaInterpretation.BucketedAccountMaster, "bar"));
        query.setRestriction(new ExistsRestriction(SchemaInterpretation.Contact, false, null));
        List<JoinSpecification> joins = query.getNecessaryJoins();
        assertEquals(joins.size(), 2);
        JoinSpecification amJoin = joins.stream()
                .filter(j -> j.getDestinationType().equals(SchemaInterpretation.BucketedAccountMaster)).findFirst()
                .orElse(null);
        assertEquals(amJoin.getDestinationObjectUsage(), ObjectUsage.LOOKUP);
        JoinSpecification contactJoin = joins.stream()
                .filter(j -> j.getDestinationType().equals(SchemaInterpretation.Contact)).findFirst().orElse(null);
        assertEquals(contactJoin.getDestinationObjectUsage(), ObjectUsage.EXISTS);
    }
}
