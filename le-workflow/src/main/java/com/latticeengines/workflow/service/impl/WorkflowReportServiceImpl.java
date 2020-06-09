package com.latticeengines.workflow.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.ReportEntityMgr;
import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.domain.exposed.workflow.Report;

@Component("workflowReportService")
public class WorkflowReportServiceImpl implements ReportService {

    @Inject
    private ReportEntityMgr reportEntityMgr;

    @Override
    public void createOrUpdateReport(Report report) {
        Report existing = reportEntityMgr.findByName(report.getName());
        if (existing != null) {
            reportEntityMgr.delete(existing);
        }
        reportEntityMgr.create(report);

    }

    @Override
    public void deleteReportByName(String name) {
        Report report = reportEntityMgr.findByName(name);
        if (report != null) {
            reportEntityMgr.delete(report);
        }
    }

    @Override
    public Report getReportByName(String name) {
        return reportEntityMgr.findByName(name);
    }

    @Override
    public List<Report> findAll() {
        return reportEntityMgr.findAll();
    }

}
