package com.latticeengines.db.exposed.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;

public interface TenantEntityMgr extends BaseEntityMgrRepository<Tenant, Long> {

    Tenant findByTenantPid(Long tenantPid);

    Tenant findByTenantId(String tenantId);

    List<String> getAllTenantId();

    Tenant findByTenantName(String tenantName);

    List<Tenant> findByTenantNamePrefix(String namePrefix);

    Tenant findBySubscriberNumber(String subscriberNumber);

    List<Tenant> findAllByStatus(TenantStatus status);

    List<Tenant> findAllByTypes(List<TenantType> types);

    void setNotificationStateByTenantId(String tenantId, String notificationLevel);

    void setNotificationTypeByTenantId(String tenantId, String notificationType);
}
