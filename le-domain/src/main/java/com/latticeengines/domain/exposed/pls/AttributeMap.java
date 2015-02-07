package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;

import com.latticeengines.common.exposed.util.JsonUtils;

public class AttributeMap extends HashMap<String, String> {

    private static final long serialVersionUID = 1835319342058481098L;

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
