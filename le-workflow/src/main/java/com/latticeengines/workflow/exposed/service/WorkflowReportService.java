package com.latticeengines.workflow.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.workflow.Report;

public interface WorkflowReportService {

    void deleteReportByName(String customerSpace, String name);

    Report findReportByName(String customerSpace, String name);

    List<Report> findAll(String customerSpace);

    void createOrUpdateReport(String customerSpace, Report report);

}
