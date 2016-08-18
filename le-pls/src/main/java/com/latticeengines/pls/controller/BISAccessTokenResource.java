package com.latticeengines.pls.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.UserService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "bisaccesstoken", description = "REST resource for getting the one-time bis access token")
@RestController
@RequestMapping("/bisaccesstoken")
public class BISAccessTokenResource {

    private static final Log log = LogFactory.getLog(BISAccessTokenResource.class);

    @Autowired
    private EmailService emailService;

    @Autowired
    private UserService userService;

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;

    private RestTemplate restTemplate = new RestTemplate();

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @SuppressWarnings("unchecked")
    @ApiOperation(value = "Get a one-time bis access token and email it to user")
    public ResponseDocument<Boolean> getOneTimeTokenAndEmail(@RequestParam String username,
            @RequestParam String tenantId) {
        try {
            User user = userService.findByUsername(username);
            String apiToken = oauth2RestApiProxy.createAPIToken(tenantId);

            log.info(
                    String.format("The user is: %s with api token: %s", user.toString(), apiToken));

            emailService.sendPlsOnetimeSfdcAccessTokenEmail(user, tenantId, apiToken);
        } catch (Exception e) {
            log.warn(String.format("Generate bis access token failed for user: %s and tenant: %s",
                    username, tenantId), e);
            return ResponseDocument.failedResponse(e);
        }
        return ResponseDocument.successResponse(true);
    }
}
