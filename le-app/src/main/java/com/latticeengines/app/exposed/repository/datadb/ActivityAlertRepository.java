package com.latticeengines.app.exposed.repository.datadb;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.cdl.activitydata.ActivityAlert;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface ActivityAlertRepository extends BaseJpaRepository<ActivityAlert, Long> {
    List<ActivityAlert> findByEntityIdAndEntityTypeAndVersionAndCategoryOrderByCreationTimestampDesc(
            @Param("entityId") String entityId, //
            @Param("entityType") BusinessEntity entityType, //
            @Param("version") String version, //
            @Param("category") AlertCategory category, //
            Pageable pageable);
}
