package com.latticeengines.db.exposed.entitymgr;


import java.util.List;

import com.latticeengines.domain.exposed.workflow.Report;

public interface ReportEntityMgr extends BaseEntityMgrRepository<Report, Long> {

    Report findByName(String name);

    // used in test to bypass multi tenant filter
    List<Report> getAll();

}
