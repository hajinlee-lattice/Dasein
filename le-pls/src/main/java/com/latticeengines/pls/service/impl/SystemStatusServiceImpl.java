package com.latticeengines.pls.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.pls.service.SystemStatusService;
import com.latticeengines.proxy.exposed.matchapi.MatchHealthProxy;

@Component("systemConfigService")
public class SystemStatusServiceImpl implements SystemStatusService {

    @Autowired
    private MatchHealthProxy matchHealthProxy;

    private Camille camille;
    private static final Logger log = LoggerFactory.getLogger(SystemStatusServiceImpl.class);
    public static final String SYSTEM_STATUS = "/systemstatus";
    public static final String PLS = "PLS";
    public static final String OK = "OK";
    public static final String UNDER_MAINTENANCE = "UNDER_MAINTENANCE";

    private Boolean isUnderMaintenance() {
        camille = CamilleEnvironment.getCamille();
        Path plsPath = PathBuilder.buildServicePath(CamilleEnvironment.getPodId(), PLS);
        plsPath = plsPath.append(SYSTEM_STATUS);
        try {
            if (camille.exists(plsPath)) {
                String zookeeperData = camille.get(plsPath).getData();
                if (!StringUtils.isBlank(zookeeperData) && zookeeperData.equals(UNDER_MAINTENANCE)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            log.error("Cannot retrieve system status from zookeeper", e);
            return false;
        }
    }

    @Override
    public StatusDocument getSystemStatus() {
        boolean underMaintenance = isUnderMaintenance();
        if (underMaintenance) {
            return StatusDocument.underMaintenance("System is under maintenance");
        }
        String rateLimitStatus = matchHealthProxy.dnbRateLimitStatus().getStatus();
        if (rateLimitStatus.equals(StatusDocument.MATCHER_IS_BUSY)) {
            return StatusDocument.matcherIsBusy("Matcher is currently busy");
        }
        return StatusDocument.ok();
    }
}
