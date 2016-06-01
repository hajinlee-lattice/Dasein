package com.latticeengines.pls.controller;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.UserService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Value;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

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
    public ResponseDocument<Boolean> getOneTimeTokenAndEmail(@RequestParam String username, @RequestParam String tenantId) {
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("TenantName", tenantId);
        requestBody.put("TenantPassword", "null");
        requestBody.put("ExternalId", "null");
        requestBody.put("JdbcDriver", "null");
        requestBody.put("JdbcUrl", "null");

        try {
            User user = userService.findByUsername(username);
            Map<String, String> response = restTemplate.postForObject(String.format("%s/tenants?TenantName=%s",
                    playmakerEndpoint, tenantId), requestBody, Map.class);

            log.info(String.format("The user is: %s", user.toString()));
            log.info(response);

            emailService.sendPlsOnetimeSfdcAccessTokenEmail(user, tenantId, response.get("TenantPassword"));
        } catch (Exception e) {
            log.warn(String.format("Generate bis access token failed for user: %s and tenant: %s", username, tenantId), e);
            return ResponseDocument.failedResponse(e);
        }
        return ResponseDocument.successResponse(true);
    }
}
