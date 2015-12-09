package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Report;

public interface ReportService {

    void deleteReportByName(String name);

    Report getReportByName(String name);

    List<Report> getAll();
    
    List<Report> findAll();

    void createOrUpdateReport(Report report);

}
