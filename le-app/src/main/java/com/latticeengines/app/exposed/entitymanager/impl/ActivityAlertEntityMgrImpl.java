package com.latticeengines.app.exposed.entitymanager.impl;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.app.exposed.entitymanager.ActivityAlertEntityMgr;
import com.latticeengines.app.exposed.repository.datadb.ActivityAlertRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("activityAlertEntityMgr")
public class ActivityAlertEntityMgrImpl extends JpaEntityMgrRepositoryImpl<ActivityAlert, Long>
        implements ActivityAlertEntityMgr {

    @Inject
    private ActivityAlertRepository activityAlertRepository;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ActivityAlert> findTopNAlertsByEntityId(String entityId, //
            BusinessEntity entityType, //
            String version, //
            AlertCategory category, //
            int limit) {
        return activityAlertRepository.findByEntityIdAndEntityTypeAndVersionAndCategoryOrderByCreationTimestampDesc(
                entityId, entityType, version, category, PageRequest.of(0, limit));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int deleteByExpireDateBefore(Date expireDate, int maxUpdateRows) {
        return activityAlertRepository.deleteByCreationTimestampBefore(expireDate, maxUpdateRows);
    }

    @Override
    public BaseJpaRepository<ActivityAlert, Long> getRepository() {
        return activityAlertRepository;
    }
}
