package com.latticeengines.pls.entitymanager;


import java.util.List;

import com.latticeengines.domain.exposed.pls.Report;

public interface ReportEntityMgr {

    void create(Report entity);

    void createOrUpdate(Report entity);

    Report findByName(String name);

    List<Report> findAll();

    void delete(Report report);

    List<Report> getAll();
}
