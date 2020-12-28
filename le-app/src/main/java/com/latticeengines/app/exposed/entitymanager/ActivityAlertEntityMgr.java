package com.latticeengines.app.exposed.entitymanager;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public interface ActivityAlertEntityMgr {

    List<ActivityAlert> findTopNAlertsByEntityId(String entityId, //
            BusinessEntity entityType, //
            String version, //
            AlertCategory Category, //
            int limit);

    int deleteByExpireDateBefore(Date expireDate, int maxUpdateRows);

    int deleteByTenantAndExpireDateBefore(Tenant tenant, Date expireDate, int maxUpdateRows);
}
