package com.latticeengines.app.exposed.repository.datadb;

import java.util.Date;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

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

    @Transactional
    @Modifying
    @Query(value = "delete from ActivityAlert where CREATION_TIMESTAMP < :creationTimestamp limit :maxUpdateRows", nativeQuery = true)
    int deleteByCreationTimestampBefore(@Param("creationTimestamp") Date creationTimestamp,
            @Param("maxUpdateRows") int max);
}
