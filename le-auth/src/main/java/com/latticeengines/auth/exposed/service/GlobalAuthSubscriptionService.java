package com.latticeengines.auth.exposed.service;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.auth.GlobalAuthSubscription;

public interface GlobalAuthSubscriptionService {

    List<String> getEmailsByTenantId(String tenantId);

    List<String> createByEmailsAndTenantId(Set<String> emails, String tenantId);

    GlobalAuthSubscription deleteByEmailAndTenantId(String email, String tenantId);

    List<String> getAllTenantId();

}
