package com.latticeengines.apps.cdl.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.auth.exposed.service.GlobalAuthSubscriptionService;
import com.latticeengines.domain.exposed.auth.GlobalAuthSubscription;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "subscription", description = "REST resource for subscription")
@RestController
@RequestMapping("/subscription")
public class SubscriptionResource {

    @Inject
    private GlobalAuthSubscriptionService subscriptionService;

    private String emailKey = "emails";

    @GetMapping("/tenant/{tenantId}")
    @ApiOperation(value = "Get subscription emails by tenantId")
    public Map<String, List<String>> getEmailsByTenantId(@PathVariable String tenantId) {
        List<String> emailList = subscriptionService.getEmailsByTenantId(CustomerSpace.parse(tenantId).toString());
        Map<String, List<String>> resultMap = new HashMap<>();
        resultMap.put(emailKey, emailList);
        return resultMap;
    }

    @PostMapping("/tenant/{tenantId}")
    @ApiOperation(value = "Create subscription by email list and tenantId")
    public Map<String, List<String>> createByEmailsAndTenantId(@PathVariable String tenantId,
            @RequestBody Map<String, Set<String>> emailsMap) {
        List<String> emailList = subscriptionService.createByEmailsAndTenantId(emailsMap.get(emailKey),
                CustomerSpace.parse(tenantId).toString());
        Map<String, List<String>> resultMap = new HashMap<>();
        resultMap.put(emailKey, emailList);
        return resultMap;
    }

    @DeleteMapping("/tenant/{tenantId}")
    @ApiOperation(value = "Delete subscription by email and tenantId")
    public void deleteSubscriptionByNameAndTenantId(@PathVariable String tenantId,
            @RequestParam(value = "email") String email) {
        GlobalAuthSubscription subscription = subscriptionService.deleteByEmailAndTenantId(email,
                CustomerSpace.parse(tenantId).toString());
        if (subscription == null) {
            throw new IllegalStateException(
                    String.format("Failed to delete: no such subscription for tenant %s, email %s", tenantId, email));
        }
    }
}
