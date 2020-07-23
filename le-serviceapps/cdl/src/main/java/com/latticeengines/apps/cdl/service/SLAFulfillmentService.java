package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.sla.SLAFulfillment;
import com.latticeengines.domain.exposed.cdl.sla.SLATerm;
import com.latticeengines.domain.exposed.pls.Action;

public interface SLAFulfillmentService {

    SLAFulfillment findByPid(String customerSpace, Long pid);

    SLAFulfillment findByActionAndTerm(String customerSpace, Action action, SLATerm term);

    List<SLAFulfillment> findByAction(String customerSpace, Action action);

    List<SLAFulfillment> findByTerm(String customerSpace, SLATerm term);

    List<SLAFulfillment> findByTenant(String customerSpace);

    SLAFulfillment createOrUpdate(String customerSpace, SLAFulfillment slaFulfillment);

    void delete(String customerSpace, SLAFulfillment slaFulfillment);
}
