package com.latticeengines.workflow.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.ReportEntityMgr;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.workflow.exposed.service.WorkflowReportService;

@Component("workflowReportService")
public class WorkflowReportServiceImpl implements WorkflowReportService {

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
    public void deleteReportByName(String customerSpace, String name) {
        Report report = reportEntityMgr.findByName(name);
        if (report != null) {
            reportEntityMgr.delete(report);
        }
    }

    @Override
    public Report getReportByName(String customerSpace, String name) {
        return reportEntityMgr.findByName(name);
    }

    @Override
    public List<Report> findAll(String customerSpace) {
        return reportEntityMgr.findAll();
    }

}
