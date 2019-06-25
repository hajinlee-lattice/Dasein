package com.latticeengines.apps.cdl.repository.reader;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.apps.cdl.repository.PlayLaunchChannelRepository;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelReaderRepository extends PlayLaunchChannelRepository {

    @Query("SELECT c FROM PlayLaunchChannel c "
            + "WHERE c.isAlwaysOn = :isAlwaysOn AND c.nextScheduledLaunch BETWEEN :startDate AND :endDate "
            + "ORDER BY c.nextScheduledLaunch")
    List<PlayLaunchChannel> findAlwaysOnChannelsByNextScheduledTime(@Param("isAlwaysOn") Boolean isAlwaysOn,
            @Param("startDate") Date startDate, @Param("endDate") Date endDate);

}
