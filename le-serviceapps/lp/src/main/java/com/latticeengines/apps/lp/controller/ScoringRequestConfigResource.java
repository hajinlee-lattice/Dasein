package com.latticeengines.apps.lp.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.apps.lp.service.ScoringRequestConfigService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "scoringrequestconfig", description = "REST resource for scoring request config")
@RestController
@RequestMapping("/scoringrequestconfig")
public class ScoringRequestConfigResource {

    private static final Logger log = LoggerFactory.getLogger(ScoringRequestConfigResource.class);

    @Inject
    private ScoringRequestConfigService scoringRequestConfigService;

    @GetMapping("/{configUuid}")
    @NoCustomerSpace
    @ApiOperation(value = "Get scoring request config by config uuid")
    public ScoringRequestConfigContext getScoringRequestConfigContext(
            @PathVariable(name = "configUuid") String configUuid) {
        if (log.isDebugEnabled()) {
            log.debug(String.format("Retrieve ScoringRequestConfiguration metadata for ConfigId: %s", configUuid));
        }
        ScoringRequestConfigContext srcContext = scoringRequestConfigService
                .retrieveScoringRequestConfigContext(configUuid);
        if (srcContext == null) {
            throw new LedpException(LedpCode.LEDP_18194, new String[]{configUuid});
        }
        return srcContext;
    }

}
