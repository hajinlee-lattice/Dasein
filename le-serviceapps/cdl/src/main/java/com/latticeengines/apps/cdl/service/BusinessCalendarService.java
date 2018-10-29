package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public interface BusinessCalendarService {

    BusinessCalendar find();

    BusinessCalendar save(BusinessCalendar calendar);

    BusinessCalendar delete();

    String validate(BusinessCalendar calendar);

}
