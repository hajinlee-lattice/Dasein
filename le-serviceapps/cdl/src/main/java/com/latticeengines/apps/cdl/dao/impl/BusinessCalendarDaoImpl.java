package com.latticeengines.apps.cdl.dao.impl;


import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.BusinessCalendarDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

@Component("businessCalendarDao")
public class BusinessCalendarDaoImpl extends BaseDaoImpl<BusinessCalendar> implements BusinessCalendarDao {

    @Override
    protected Class<BusinessCalendar> getEntityClass() {
        return BusinessCalendar.class;
    }

}

