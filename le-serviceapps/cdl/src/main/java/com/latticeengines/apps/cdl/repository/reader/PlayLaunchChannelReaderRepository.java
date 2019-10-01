package com.latticeengines.apps.cdl.repository.reader;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.apps.cdl.repository.PlayLaunchChannelRepository;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelReaderRepository extends PlayLaunchChannelRepository {

    @Query("SELECT c FROM PlayLaunchChannel c INNER JOIN c.play p LEFT JOIN p.ratingEngine r "
            + "WHERE c.isAlwaysOn = true " + "AND c.nextScheduledLaunch BETWEEN :startDate AND :endDate "
            + "AND p.deleted = false " + "AND (p.ratingEngine is null OR r.status = 'ACTIVE') "
            + "ORDER BY c.nextScheduledLaunch")
    List<PlayLaunchChannel> findAlwaysOnChannelsByNextScheduledTime(@Param("startDate") Date startDate,
            @Param("endDate") Date endDate);

}
