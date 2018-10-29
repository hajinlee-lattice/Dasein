package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.BusinessCalendarEntityMgr;
import com.latticeengines.apps.cdl.service.BusinessCalendarService;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.util.BusinessCalendarUtils;


@Service("businessCalendarService")
public class BusinessCalendarServiceImpl implements BusinessCalendarService {

    @Inject
    private BusinessCalendarEntityMgr entityMgr;

    @Override
    public BusinessCalendar find() {
        return entityMgr.find();
    }

    @Override
    public BusinessCalendar save(BusinessCalendar calendar) {
        BusinessCalendarUtils.validate(calendar);
        return entityMgr.save(calendar);
    }

    @Override
    public BusinessCalendar delete() {
        return entityMgr.delete();
    }

    @Override
    public String validate(BusinessCalendar calendar) {
        return BusinessCalendarUtils.validate(calendar);
    }

}
