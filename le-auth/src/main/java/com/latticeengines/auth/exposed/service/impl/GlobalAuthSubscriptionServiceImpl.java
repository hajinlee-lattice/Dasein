package com.latticeengines.auth.exposed.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthSubscriptionEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.auth.exposed.service.GlobalAuthSubscriptionService;
import com.latticeengines.domain.exposed.auth.GlobalAuthSubscription;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

@Component("globalAuthSubscriptionService")
public class GlobalAuthSubscriptionServiceImpl implements GlobalAuthSubscriptionService {

    private static final Logger log = LoggerFactory.getLogger(GlobalTeamManagementServiceImpl.class);

    @Inject
    private GlobalAuthSubscriptionEntityMgr globalAuthSubscriptionEntityMgr;

    @Inject
    private GlobalAuthUserTenantRightEntityMgr globalAuthUserTenantRightEntityMgr;

    @Inject
    private GlobalAuthTenantEntityMgr gaTenantEntityMgr;

    @Override
    public List<String> getEmailsByTenantId(String tenantId) {
        return globalAuthSubscriptionEntityMgr.findEmailsByTenantId(tenantId);
    }

    @Override
    // return emails that are successfully created or already subscribed
    public List<String> createByEmailsAndTenantId(Set<String> emails, String tenantId) {
        List<String> successEmail = new ArrayList<>();
        GlobalAuthTenant gaTenantData = gaTenantEntityMgr.findByTenantId(tenantId);
        if (CollectionUtils.isNotEmpty(emails) && gaTenantData != null) {
            List<GlobalAuthSubscription> subscriptionList = new ArrayList<>();
            List<GlobalAuthUserTenantRight> userTenantRightLists = globalAuthUserTenantRightEntityMgr
                    .findByEmailsAndTenantId(emails, gaTenantData.getPid());
            GlobalAuthSubscription subscription;
            for (GlobalAuthUserTenantRight userTenantRight : userTenantRightLists) {
                if (globalAuthSubscriptionEntityMgr.findByUserTenantRight(userTenantRight) == null) {
                    subscription = new GlobalAuthSubscription();
                    subscription.setTenantId(tenantId);
                    subscription.setUserTenantRight(userTenantRight);
                    subscriptionList.add(subscription);
                }
                successEmail.add(userTenantRight.getGlobalAuthUser().getEmail());
            }
            globalAuthSubscriptionEntityMgr.create(subscriptionList);
        }
        return successEmail;
    }

    @Override
    public GlobalAuthSubscription deleteByEmailAndTenantId(String email, String tenantId) {
        GlobalAuthSubscription subscription = globalAuthSubscriptionEntityMgr.findByEmailAndTenantId(email, tenantId);
        if (subscription != null) {
            globalAuthSubscriptionEntityMgr.delete(subscription);
            log.info(String.format("delete subscription for tenant : %s , emails is %s.", tenantId, email));
        }
        return subscription;
    }
}
