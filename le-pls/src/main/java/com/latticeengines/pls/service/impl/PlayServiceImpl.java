package com.latticeengines.pls.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.plexus.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("playService")
public class PlayServiceImpl implements PlayService {

    private static Log log = LogFactory.getLog(PlayServiceImpl.class);

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public Play createPlay(Play play, String tenantId) {
        log.info(String.format("Creating play with name: %s, segment name: %s, on tenant %s", play.getName(),
                play.getSegmentName(), tenantId));
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        play.setTenant(tenant);
        playEntityMgr.create(play);
        return play;
    }

    @Override
    public List<Play> getAllPlays() {
        return playEntityMgr.findAll();
    }

    @Override
    public Play getPlayByName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        return playEntityMgr.findByName(name);
    }

    @Override
    public void deleteByName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new LedpException(LedpCode.LEDP_18144);
        }
        playEntityMgr.deleteByName(name);
    }

}
