package com.latticeengines.auth.exposed.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTeamEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.cdl.CDLObjectTypes;
import com.latticeengines.domain.exposed.graph.IdToDisplayNameTranslator;
import com.latticeengines.domain.exposed.graph.VertexType;

@Component
public class GlobalAuthIdToDisplayNameTranslator extends IdToDisplayNameTranslator {

    @Inject
    private GlobalAuthTeamEntityMgr globalAuthTeamEntityMgr;

    @Inject
    private GlobalAuthTenantEntityMgr globalAuthTenantEntityMgr;

    @Override
    public String idToDisplayName(String type, String objId) {
        String tenantId = MultiTenantContext.getTenant().getId();
        GlobalAuthTenant globalAuthTenant = globalAuthTenantEntityMgr.findByTenantId(tenantId);
        if (globalAuthTenant == null) {
            throw new IllegalArgumentException(String.format("cannot find globalAuthTenant using tenant id %s.", tenantId));
        }
        String displayName = objId;
        if (type.equals(VertexType.TEAM)) {
            GlobalAuthTeam globalAuthTeam = globalAuthTeamEntityMgr.findByTeamIdAndTenantId(globalAuthTenant.getPid(), objId);
            if (globalAuthTeam != null) {
                displayName = globalAuthTeam.getName();
            }
        }
        return displayName;
    }

    @Override
    public String translateType(String vertexType) {
        String translatedType = vertexType;
        if (vertexType.equals(VertexType.TEAM)) {
            translatedType = CDLObjectTypes.Team.name();
        }
        return translatedType;
    }

    @Override
    public String toVertexType(String objectType) {
        String translatedType = null;
        CDLObjectTypes type = CDLObjectTypes.valueOf(objectType);
        if (type != null) {
            if (type == CDLObjectTypes.Team) {
                translatedType = VertexType.TEAM;
            }
        }
        return translatedType;
    }
}
