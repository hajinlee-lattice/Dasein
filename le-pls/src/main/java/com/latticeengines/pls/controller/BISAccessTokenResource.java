package com.latticeengines.pls.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "bisaccesstoken", description = "REST resource for getting the one-time bis access token")
@RestController
@RequestMapping("/bisaccesstoken")
@PreAuthorize("hasAnyRole('Create_PLS_Oauth2Token','Create_PLS_Oauth2Token_External')")
public class BISAccessTokenResource {

    private static final Logger log = LoggerFactory.getLogger(BISAccessTokenResource.class);

    @Inject
    private EmailService emailService;

    @Inject
    private UserService userService;

    @Inject
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @GetMapping
    @ResponseBody
    @ApiOperation(value = "Get a one-time bis access token and email it to user")
    public ResponseDocument<Boolean> getOneTimeTokenAndEmail(@RequestParam String username,
            @RequestParam String tenantId) {
        try {
            User user = userService.findByUsername(username);
            AccessLevel level = userService.getAccessLevel(tenantId, username);
            if (level == null || level.compareTo(AccessLevel.EXTERNAL_USER) < 0) {
                throw new LedpException(LedpCode.LEDP_18147, new String[]{username, tenantId});
            }

            String apiToken = oauth2RestApiProxy.createAPIToken(tenantId);

            log.info(
                    String.format("The user is: %s with api token: %s", user.toString(), apiToken));

            emailService.sendPlsOnetimeSfdcAccessTokenEmail(user, tenantId, apiToken);
        } catch (Exception e) {
            log.warn(String.format("Generate bis access token failed for user: %s and tenant: %s",
                    username, tenantId));
            return ResponseDocument.failedResponse(e);
        }
        return ResponseDocument.successResponse(true);
    }
}
