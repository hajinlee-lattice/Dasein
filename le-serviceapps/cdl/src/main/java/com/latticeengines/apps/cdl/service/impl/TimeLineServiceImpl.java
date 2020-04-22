package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.TimeLineEntityMgr;
import com.latticeengines.apps.cdl.service.TimeLineService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

import io.micrometer.core.instrument.util.StringUtils;

@Service("timeLineService")
public class TimeLineServiceImpl implements TimeLineService {

    private static final Logger log = LoggerFactory.getLogger(TimeLineServiceImpl.class);

    @Inject
    private TimeLineEntityMgr timeLineEntityMgr;

    @Override
    public TimeLine findByPid(String customerSpace, Long pid) {
        return timeLineEntityMgr.findByPid(pid);
    }

    @Override
    public TimeLine findByTimelineId(String customerSpace, String timelineId) {
        return timeLineEntityMgr.findByTimeLineId(timelineId);
    }

    @Override
    public List<TimeLine> findByTenant(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        return timeLineEntityMgr.findByTenant(tenant);
    }

    @Override
    public TimeLine findByTenantAndEntity(String customerSpace, BusinessEntity entity) {
        return timeLineEntityMgr.findByEntity(entity.name());
    }

    public TimeLine createOrUpdateTimeLine(String customerSpace, TimeLine timeLine) {
        String uniqueId = timeLine.getTimelineId();
        TimeLine newTimeLine = null;
        if (StringUtils.isNotEmpty(uniqueId)) {
            newTimeLine = timeLineEntityMgr.findByTimeLineId(uniqueId);
        }
        if (newTimeLine == null) {
            newTimeLine = new TimeLine();
            newTimeLine.setTimelineId(TimeLine.generateId());
            newTimeLine.setTenant(MultiTenantContext.getTenant());
        }
        newTimeLine.setEntity(timeLine.getEntity());
        newTimeLine.setEventMappings(timeLine.getEventMappings());
        newTimeLine.setName(timeLine.getName());
        newTimeLine.setStreamTypes(timeLine.getStreamTypes());
        newTimeLine.setStreamIds(timeLine.getStreamIds());
        timeLineEntityMgr.createOrUpdate(newTimeLine);
        return newTimeLine;
    }

    @Override
    public void delete(String customerSpace, TimeLine timeLine) {
        TimeLine oldTimeline = timeLineEntityMgr.findByPid(timeLine.getPid());
        if (oldTimeline == null) {
            log.error("cannot find timeline in tenant {}, timeline name is {}, timeline_id is {}.", customerSpace,
                    timeLine.getName(), timeLine.getTimelineId());
            return;
        }
        timeLineEntityMgr.delete(timeLine);
    }

}
