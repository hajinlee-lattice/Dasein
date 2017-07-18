package com.latticeengines.domain.exposed.util;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public final class ReverseQueryTranslator {

    public static FrontEndRestriction translateRestriction(Restriction restriction) {
        if (restriction == null) {
            return null;
        }
        // clone
        restriction = JsonUtils.deserialize(JsonUtils.serialize(restriction), Restriction.class);

        BreadthFirstSearch search = new BreadthFirstSearch();
        search.run(restriction, (object, ctx) -> {
            if (object instanceof ConcreteRestriction) {
                ConcreteRestriction concrete = (ConcreteRestriction) object;
                BucketRestriction bucket = BucketRestriction.from(concrete);
                LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                parent.getRestrictions().remove(concrete);
                parent.getRestrictions().add(bucket);
            }
        });

        FrontEndRestriction result = new FrontEndRestriction();
        result.setRestriction(restriction);
        return result;
    }

    //TODO: may need a method to combine restriction and statistics to populate bktId

}
