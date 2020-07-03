package com.latticeengines.apps.cdl.repository.reader;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.apps.cdl.repository.PlayLaunchChannelRepository;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelReaderRepository extends PlayLaunchChannelRepository {

    @Query("SELECT c FROM PlayLaunchChannel c " //
            + "INNER JOIN c.play p " //
            + "LEFT JOIN p.ratingEngine r " //
            + "LEFT JOIN WorkflowJob w on w.pid = c.lastDeltaWorkflowId " //
            + "WHERE c.isAlwaysOn = true " //
            + "AND c.expirationDate > CURRENT_TIMESTAMP() " //
            + "AND c.nextScheduledLaunch BETWEEN :startDate AND :endDate " //
            + "AND p.deleted = false " //
            + "AND (p.ratingEngine is null OR r.status = 'ACTIVE') " //
            + "AND (c.lastDeltaWorkflowId is null OR (w.status NOT IN ('ENQUEUED', 'PENDING', 'RUNNING'))) " //
            + "ORDER BY c.nextScheduledLaunch")
    List<PlayLaunchChannel> findAlwaysOnChannelsByNextScheduledTime(@Param("startDate") Date startDate,
            @Param("endDate") Date endDate);

    @Query("SELECT c FROM PlayLaunchChannel c INNER JOIN c.play p WHERE " + //
            "p.deleted = false " + //
            "AND c.lookupIdMap.externalSystemName = :externalSystemName " + //
            "AND c.isAlwaysOn = false " +
            "AND c.channelConfig LIKE %:attributeSetName%")
    List<PlayLaunchChannel> findByIsAlwaysOnFalseAnAndLookupIdMap(@Param("externalSystemName") CDLExternalSystemName externalSystemName,
                                                                  @Param("attributeSetName") String attributeSetName);

}
