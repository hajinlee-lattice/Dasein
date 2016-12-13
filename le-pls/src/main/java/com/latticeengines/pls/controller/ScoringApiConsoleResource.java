package com.latticeengines.pls.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.network.exposed.scoringapi.InternalScoringApiInterface;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "scores/apiconsole", description = "REST resource for interacting with scoringapi via APIConsole")
@RestController
@RequestMapping("/scores/apiconsole")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoringApiConsoleResource {

    @Autowired
    protected InternalScoringApiInterface internalScoringApiProxy;

    @Autowired
    private SessionService sessionService;

    @RequestMapping(value = "/record/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a record including debug info such as probability via APIConsole")
    public DebugScoreResponse scoreAndEnrichRecordApiConsole(HttpServletRequest request, //
            @RequestBody ScoreRequest scoreRequest) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        boolean enrichmentEnabledForInternalAttributes = FeatureFlagClient.isEnabled(
                CustomerSpace.parse(tenant.getId()),
                LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES.getName());
        return internalScoringApiProxy.scoreAndEnrichRecordApiConsole(scoreRequest, tenant.getId(),
                enrichmentEnabledForInternalAttributes);
    }
}
