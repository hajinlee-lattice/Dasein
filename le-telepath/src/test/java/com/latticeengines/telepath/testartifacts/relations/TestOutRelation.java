package com.latticeengines.telepath.testartifacts.relations;

import com.latticeengines.telepath.relations.BaseRelation;
import com.latticeengines.telepath.relations.Relation;

public class TestOutRelation extends BaseRelation implements Relation {

    public static final String TYPE = "__test_outward";

    @Override
    public boolean isOutwardRelation() {
        return true;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
