package com.latticeengines.auth.aspect;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTeamEntityMgr;
import com.latticeengines.auth.exposed.service.impl.GlobalAuthIdToDisplayNameTranslator;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.graphdb.DependenciesToGraphAction;
import com.latticeengines.graphdb.util.DependencyUtils;

@Component
public class GlobalAuthDependenciesToGraphAction extends DependenciesToGraphAction {

    @Inject
    private GlobalAuthTeamEntityMgr globalAuthTeamEntityMgr;

    @Inject
    private GlobalAuthIdToDisplayNameTranslator nameTranslator;


    public void createTeamVertex(GlobalAuthTeam globalAuthTeam) throws Exception {
        ParsedDependencies parsedDependencies = globalAuthTeamEntityMgr.parse(globalAuthTeam, null);
        try {
            beginThreadLocalCluster();
            createVertex(MultiTenantContext.getTenant().getId(), parsedDependencies, globalAuthTeam.getTeamId(),
                    VertexType.TEAM, null, null);
        } finally {
            closeThreadLocalCluster();
        }
    }

    public void checkDeleteSafety(String vertexId, String vertexType) throws Exception {
        List<Map<String, String>> dependencies = //
                checkDirectDependencies(MultiTenantContext.getTenant().getId(), //
                        vertexId, vertexType);
        DependencyUtils.checkDeleteSafety(dependencies, nameTranslator, vertexId, vertexType);
    }


}
