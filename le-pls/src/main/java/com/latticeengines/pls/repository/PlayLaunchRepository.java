package com.latticeengines.pls.repository;

import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.PagingAndSortingRepository;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

public interface PlayLaunchRepository extends PagingAndSortingRepository<PlayLaunch, Long>, JpaSpecificationExecutor<PlayLaunch>{
	
	PlayLaunch findByLaunchId(String launchId);
	
	PlayLaunch findByPlayAndCreated(Long playId, Date timestamp);
	
	List<PlayLaunch> findByPlayPidAndLaunchStateInOrderByCreatedDesc(Long playId, List<LaunchState> states);
	
    PlayLaunch findTopByPlayPidAndLaunchStateInOrderByCreatedDesc(Long playId, List<LaunchState> states);
    
    List<PlayLaunch> findByLaunchStateOrderByCreatedDesc(LaunchState state);
    /*
    List<PlayLaunch> findByPlayStatesAndPagination(Long playId, List<LaunchState> states, Long startTimestamp,
            Long offset, Long max, Long endTimestamp);

    Long findCountByPlayStatesAndTimestamps(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp);

    Stats findTotalCountByPlayStatesAndTimestamps(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp);
	*/
}
