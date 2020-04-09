package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.activity.TimeLine;

public interface TimeLineService {

    TimeLine findByPid(String customerSpace, Long pid);

    TimeLine findByTimelineId(String customerSpace, String timelineId);

    List<TimeLine> findByTenant(String customerSpace);

    TimeLine createOrUpdateTimeLine(String customerSpace, TimeLine timeLine);

    void delete(String customerSpace, TimeLine timeLine);
}
