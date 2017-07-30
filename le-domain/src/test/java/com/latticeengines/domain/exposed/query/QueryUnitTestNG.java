package com.latticeengines.domain.exposed.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.graph.utils.GraphUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class QueryUnitTestNG {

    /**
     * Simple test to assert that the Query interface will not change
     */
    @Test(groups = "unit", enabled = false)
    public void testStableInterface() {
        String serialized = "{\"lookups\":null,\"restriction\":{\"logicalRestriction\":{\"operator\":\"AND\",\"restrictions\":[{\"logicalRestriction\":{\"operator\":\"OR\",\"restrictions\":[],\"childMap\":null,\"children\":[]}},{\"logicalRestriction\":{\"operator\":\"AND\",\"restrictions\":[],\"childMap\":null,\"children\":[]}}],\"childMap\":null,\"children\":[{\"operator\":\"OR\",\"restrictions\":[],\"childMap\":null,\"children\":[]},{\"operator\":\"AND\",\"restrictions\":[],\"childMap\":null,\"children\":[]}]}},\"sort\":null,\"object_type\":\"Account\",\"page_filter\":null,\"free_form_text_search\":null}";
        Query query = JsonUtils.deserialize(serialized, Query.class);
        assertNotNull(query);
    }

    @Test(groups = "unit")
    public void testSeDer() {
        Query query = getEntityQuery();
        String serialized = JsonUtils.serialize(query);
        Query deserialized = JsonUtils.deserialize(serialized, Query.class);
        Assert.assertNotNull(deserialized);
        System.out.println(JsonUtils.serialize(query));
    }

    @Test(groups = "unit")
    public void testSelectEntity() {
        Query query = getEntityQuery();
        query.analyze();
        assertEquals(query.getLookupJoins().size(), 1);
        assertEquals(query.getExistsJoins().size(), 1);
        JoinSpecification amJoin = query.getLookupJoins().stream()
                .filter(j -> j.getDestinationEntity().equals(BusinessEntity.LatticeAccount)).findFirst().orElse(null);
        assertEquals(amJoin.getDestinationObjectUsage(), ObjectUsage.LOOKUP);
        JoinSpecification contactJoin = query.getExistsJoins().stream()
                .filter(j -> j.getDestinationEntity().equals(BusinessEntity.Contact)).findFirst().orElse(null);
        assertEquals(contactJoin.getDestinationObjectUsage(), ObjectUsage.EXISTS);
    }

    @Test(groups = "unit")
    public void testGetAllOfType() {
        Query query = getEntityQuery();
        List<AttributeLookup> lookups = GraphUtils.getAllOfType(query, AttributeLookup.class);
        assertEquals(lookups.size(), 2);
    }

    @Test(groups = "unit")
    public void testGetAllOfTypeComplexRestriction() {
        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.LatticeAccount, "TechIndicator_AdRoll").eq("Yes") //
                .build();

        LogicalRestriction logic1 = new LogicalRestriction();
        logic1.setOperator(LogicalOperator.AND);
        logic1.addRestriction(restriction);

        LogicalRestriction logic2 = new LogicalRestriction();
        logic2.setOperator(LogicalOperator.OR);

        LogicalRestriction logic3 = new LogicalRestriction();
        logic3.setOperator(LogicalOperator.AND);
        logic3.addRestriction(logic2);
        logic3.addRestriction(logic1);

        String json = JsonUtils.serialize(logic3);
        Restriction deserialized = JsonUtils.deserialize(json, Restriction.class);
        List<AttributeLookup> lookups = GraphUtils.getAllOfType(deserialized, AttributeLookup.class);
        assertEquals(lookups.size(), 1);
    }

    private Query getEntityQuery() {
        return Query.builder() //
                .select(BusinessEntity.Account, "foo") //
                .select(BusinessEntity.LatticeAccount, "bar") //
                .exist(BusinessEntity.Contact) //
                .build();
    }
}
