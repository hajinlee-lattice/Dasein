package com.latticeengines.domain.exposed.query;

import static org.testng.Assert.assertNotNull;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

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
}
