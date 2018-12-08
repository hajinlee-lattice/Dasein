package com.latticeengines.matchapi.controller;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of match API")
@RestController
@RequestMapping("/health")
public class HealthResource {

    private static final Logger log = LoggerFactory.getLogger(HealthResource.class);

    private static final AtomicBoolean ready = new AtomicBoolean(false);

    @Inject
    private RateLimitingService ratelimitingService;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Inject
    private RealTimeMatchService realTimeMatchService;

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    public StatusDocument healthCheck() {
        if (!ready.get()) {
            getReady();
        }
        return StatusDocument.online();
    }

    @GetMapping("/dnbstatus")
    @ResponseBody
    @ApiOperation(value = "DnB Rate Limit Status")
    public StatusDocument dnbRateLimitStatus() {
        boolean dnbAvailable = ratelimitingService.acquireDnBBulkRequest(1, true).isAllowed();
        if (dnbAvailable) {
            return StatusDocument.ok();
        } else {
            return StatusDocument.matcherIsBusy();
        }
    }

    private synchronized void getReady() {
        if (!ready.get()) {
            boolean matchSuccessful = false;
            while (!matchSuccessful) {
                try {
                    runMatch();
                    matchSuccessful = true;
                } catch (Exception e) {
                    log.warn("MatchAPI is not ready." + e.getMessage());
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("Readiness check is interrupted.");
                }
            }
            log.info("MatchAPI is ready.");
            ready.set(true);
        }
    }

    private void runMatch() {
        MatchInput matchInput = testMatchInput();
        MatchOutput matchOutput = realTimeMatchService.match(matchInput);
        if (Boolean.TRUE.equals(matchOutput.getResult().get(0).isMatched())) {
            log.info("Matched lattice-engines.com to " + JsonUtils.serialize(matchOutput.getResult().get(0).getOutput().get(0)));
        } else {
            log.warn("lattice-engines.com is not matched!");
        }
    }

    private MatchInput testMatchInput() {
        MatchInput matchInput = new MatchInput();
        matchInput.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        matchInput.setFields(Collections.singletonList("Domain"));
        matchInput.setData(Collections.singletonList(Collections.singletonList("lattice-engines.com")));
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(Collections.singletonList(new Column("LDC_Name")));
        matchInput.setCustomSelection(cs);
        return matchInput;
    }
}
