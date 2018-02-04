package com.latticeengines.db.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.workflow.Report;

public interface ReportService {

    void deleteReportByName(String name);

    Report getReportByName(String name);

    List<Report> findAll();

    void createOrUpdateReport(Report report);

}
