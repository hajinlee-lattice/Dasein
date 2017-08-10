package com.latticeengines.apps.lp.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.apps.lp.service.OneTimePasswordService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of service app")
@RestController
@RequestMapping("/oauthotps")
public class OneTimePasswordResource {

    private final OneTimePasswordService oneTimePasswordService;

    @Inject
    public OneTimePasswordResource(OneTimePasswordService oneTimePasswordService) {
        this.oneTimePasswordService = oneTimePasswordService;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate a oauth2 one time password")
    @NoMetricsLog
    @NoCustomerSpace
    public ResponseDocument<String> generateOTP(@RequestParam(value = "user") String user) {
        String password = oneTimePasswordService.generateOTP(user);
        return ResponseDocument.successResponse(password);
    }

}
