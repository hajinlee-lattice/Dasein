package com.latticeengines.telepath.testartifacts.relations;

import com.latticeengines.telepath.relations.BaseRelation;
import com.latticeengines.telepath.relations.Relation;

public class TestInRelation extends BaseRelation implements Relation {

    public static final String TYPE = "__test_inward";

    @Override
    public boolean isOutwardRelation() {
        return false;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
