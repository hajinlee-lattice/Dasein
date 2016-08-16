package com.latticeengines.pls.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.domain.exposed.security.User;
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

    @Value("${pls.sureshot.playmaker.endpoint}")
    private String playmakerEndpoint;

    private RestTemplate restTemplate = new RestTemplate();

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @SuppressWarnings("unchecked")
    @ApiOperation(value = "Get a one-time bis access token and email it to user")
    public ResponseDocument<Boolean> getOneTimeTokenAndEmail(@RequestParam String username,
            @RequestParam String tenantId) {
        PlaymakerTenant playmakerTenant = new PlaymakerTenant();
        playmakerTenant.setTenantName(tenantId);
        playmakerTenant.setTenantPassword("null");
        playmakerTenant.setExternalId("null");
        playmakerTenant.setJdbcDriver("null");
        playmakerTenant.setJdbcUrl("null");

        try {
            User user = userService.findByUsername(username);
            playmakerTenant = restTemplate.postForObject(
                    String.format("%s/tenants", playmakerEndpoint), playmakerTenant,
                    PlaymakerTenant.class);

            log.info(String.format("The user is: %s", user.toString()));
            log.info(playmakerTenant);

            emailService.sendPlsOnetimeSfdcAccessTokenEmail(user, tenantId,
                    playmakerTenant.getTenantPassword());
        } catch (Exception e) {
            log.warn(String.format("Generate bis access token failed for user: %s and tenant: %s",
                    username, tenantId), e);
            return ResponseDocument.failedResponse(e);
        }
        return ResponseDocument.successResponse(true);
    }
}
