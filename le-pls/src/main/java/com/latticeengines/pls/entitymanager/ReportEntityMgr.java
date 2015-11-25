package com.latticeengines.pls.entitymanager;


import com.latticeengines.domain.exposed.pls.Report;

import java.util.List;

public interface ReportEntityMgr {

    void create(Report entity);

    void createOrUpdate(Report entity);

    Report findByGuid(String guid);

    List<Report> findAll();

    void delete(Report report);
}
