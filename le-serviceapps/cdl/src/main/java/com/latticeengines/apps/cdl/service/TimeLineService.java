package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface TimeLineService {

    TimeLine findByPid(String customerSpace, Long pid);

    TimeLine findByTimelineId(String customerSpace, String timelineId);

    TimeLine findByTenantAndEntity(String customerSpace, BusinessEntity entity);

    List<TimeLine> findByTenant(String customerSpace);

    TimeLine createOrUpdateTimeLine(String customerSpace, TimeLine timeLine);

    void delete(String customerSpace, TimeLine timeLine);
}
