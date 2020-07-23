package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.sla.SLATerm;

public interface SLATermService {

    SLATerm findByPid(String customerSpace, Long pid);

    SLATerm findByTermName(String customerSpace, String termName);

    List<SLATerm> findByTenant(String customerSpace);

    SLATerm createOrUpdate(String customerSpace, SLATerm slaTerm);

    void delete(String customerSpace, SLATerm slaTerm);
}
