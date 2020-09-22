package com.latticeengines.apps.cdl.controller;

import java.util.List;
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

    @GetMapping("/tenant/{tenantId}")
    @ApiOperation(value = "Get subscription emails by tenantId")
    public List<String> getEmailsByTenantId(@PathVariable String tenantId,
            @RequestParam(value = "contractId") String contractId) {
        CustomerSpace space = new CustomerSpace(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        return subscriptionService.getEmailsByTenantId(space.toString());
    }

    @PostMapping("/tenant/{tenantId}")
    @ApiOperation(value = "Create subscription by email list and tenantId")
    public List<String> createByEmailsAndTenantId(@PathVariable String tenantId,
            @RequestParam(value = "contractId") String contractId, @RequestBody Set<String> emails) {
        CustomerSpace space = new CustomerSpace(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        return subscriptionService.createByEmailsAndTenantId(emails, space.toString());
    }

    @DeleteMapping("/tenant/{tenantId}")
    @ApiOperation(value = "Delete subscription by email and tenantId")
    public void deleteSubscriptionByNameAndTenantId(@PathVariable String tenantId,
            @RequestParam(value = "contractId") String contractId, @RequestParam(value = "email") String email) {
        CustomerSpace space = new CustomerSpace(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        GlobalAuthSubscription subscription = subscriptionService.deleteByEmailAndTenantId(email, space.toString());
        if (subscription == null) {
            throw new IllegalStateException(String
                    .format("Failed to delete: no such subscription for tenant %s, email %s", space.toString(), email));
        }
    }
}
