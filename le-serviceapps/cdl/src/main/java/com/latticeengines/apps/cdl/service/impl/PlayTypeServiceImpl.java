package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.PlayTypeEntityMgr;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("playTypeService")
public class PlayTypeServiceImpl implements PlayTypeService {
    private static final Logger log = LoggerFactory.getLogger(PlayTypeService.class);

    @Inject
    private PlayTypeEntityMgr playTypeEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Value("${cdl.play.service.default.types}")
    private String defaultPlayTypes;

    @Value("${cdl.play.service.default.types.user}")
    private String defaultPlayTypeUser;

    public List<PlayType> getAllPlayTypes(String customerSpace) {
        List<PlayType> types = playTypeEntityMgr.findAll();
        if (CollectionUtils.isEmpty(types)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
            if (tenant == null) {
                throw new LedpException(LedpCode.LEDP_38008);
            }
            log.info("No types found for the tenant " + customerSpace);
            createDefaultPlayTypes(tenant);
            types = playTypeEntityMgr.findAll();
        }
        return types;
    }

    private void createDefaultPlayTypes(Tenant tenant) {
        log.info("Creating the following default types " + defaultPlayTypes + " for tenant: " + tenant.getId());
        List<String> defaultTypes = Arrays.asList(defaultPlayTypes.split(","));
        defaultTypes.forEach(type -> playTypeEntityMgr
                .create(new PlayType(tenant, type, null, defaultPlayTypeUser, defaultPlayTypeUser)));
    }
}
