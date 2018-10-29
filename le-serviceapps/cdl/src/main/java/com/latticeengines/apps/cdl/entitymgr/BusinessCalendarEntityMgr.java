package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

public interface BusinessCalendarEntityMgr extends BaseEntityMgrRepository<BusinessCalendar, Long>  {

    BusinessCalendar save(BusinessCalendar businessCalendar);

    BusinessCalendar find();

    BusinessCalendar delete();
}
