package com.latticeengines.workflow.exposed.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.workflow.exposed.entitymanager.ReportEntityMgr;
import com.latticeengines.workflow.exposed.service.ReportService;

@Component("reportService")
public class ReportServiceImpl implements ReportService {

    @Autowired
    private ReportEntityMgr reportEntityMgr;

    @Override
    public void createOrUpdateReport(Report report) {
        Report existing = reportEntityMgr.findByName(report.getName());
        if (existing != null) {
            reportEntityMgr.delete(report);
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
    public List<Report> getAll() {
        return reportEntityMgr.getAll();
    }

    @Override
    public List<Report> findAll() {
        return reportEntityMgr.findAll();
    }

}
