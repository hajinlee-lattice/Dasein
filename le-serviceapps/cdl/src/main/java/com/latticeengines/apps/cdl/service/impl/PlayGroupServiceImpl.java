package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayGroupEntityMgr;
import com.latticeengines.apps.cdl.service.PlayGroupService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayGroup;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("playGroupService")
public class PlayGroupServiceImpl implements PlayGroupService {
    private static final Logger log = LoggerFactory.getLogger(PlayGroupService.class);

    @Inject
    private PlayGroupEntityMgr playGroupEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Override
    public List<PlayGroup> getAllPlayGroups(String customerSpace) {
        List<PlayGroup> groups = playGroupEntityMgr.findAll();
        if (CollectionUtils.isEmpty(groups)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
            if (tenant == null) {
                throw new LedpException(LedpCode.LEDP_38008);
            }
            log.info("No Groups found for the tenant " + customerSpace);
            groups = playGroupEntityMgr.findAll();
        }
        return groups;
    }

    @Override
    public void deletePlayGroupsFromPlays(String customerSpace, PlayGroup playGroup) {
        Hibernate.initialize(playGroup.getPlays());
        for (Play play : playGroup.getPlays()) {
            play.getPlayGroups().remove(playGroup);
            playEntityMgr.update(play);
        }
        playGroupEntityMgr.delete(playGroup);
    }
}
