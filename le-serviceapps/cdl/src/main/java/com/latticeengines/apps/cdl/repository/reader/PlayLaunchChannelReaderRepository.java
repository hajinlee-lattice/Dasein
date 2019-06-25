package com.latticeengines.apps.cdl.repository.reader;

import com.latticeengines.apps.cdl.repository.PlayLaunchChannelRepository;

public interface PlayLaunchChannelReaderRepository extends PlayLaunchChannelRepository {

    // @Query("SELECT c FROM playLaunchChannel c "
    // + "WHERE c.isAlwaysOn = ?1 AND c.nextScheduledLaunch < DATE_ADD(CURRENT_TIME(), INTERVAL 15 MINUTE) "
    // + "ORDER BY c.nextScheduledLaunch")
    // List<PlayLaunchChannel> findChannelsToLaunchInNext15Mins(Boolean isAlwaysOn);

}
