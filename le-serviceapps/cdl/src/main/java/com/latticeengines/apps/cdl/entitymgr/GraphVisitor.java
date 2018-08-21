package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.security.Tenant;

public interface GraphVisitor {

    void visit(RatingEngine entity, //
            ParsedDependencies parsedDependencies) throws Exception;

    void visit(AIModel entity, //
            ParsedDependencies parsedDependencies) throws Exception;

    void visit(RuleBasedModel entity, //
            ParsedDependencies parsedDependencies) throws Exception;

    void visit(MetadataSegment entity, //
            ParsedDependencies parsedDependencies) throws Exception;

    void visit(Play entity, //
            ParsedDependencies parsedDependencies) throws Exception;

    void populateTenantGraph(Tenant tenant) throws Exception;
}
