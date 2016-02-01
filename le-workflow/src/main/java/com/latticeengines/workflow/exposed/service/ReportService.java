package com.latticeengines.workflow.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.workflow.Report;

public interface ReportService {

    void deleteReportByName(String name);

    Report getReportByName(String name);

    List<Report> getAll();

    List<Report> findAll();

    void createOrUpdateReport(Report report);

}
