package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.service.ReportService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Service("reportService")
public class ReportServiceImpl implements ReportService {

    @Inject
    private WorkflowProxy workflowProxy;

    @Override
    public void createOrUpdateReport(Report report) {
        workflowProxy.registerReport(MultiTenantContext.getTenant().getId(), report);
    }

    @Override
    public void deleteReportByName(String name) {
        workflowProxy.deleteReportByName(MultiTenantContext.getTenant().getId(), name);
    }

    @Override
    public Report getReportByName(String name) {
        return workflowProxy.findReportByName(MultiTenantContext.getTenant().getId(), name);
    }

    @Override
    public List<Report> findAll() {
        return workflowProxy.findAllReports(MultiTenantContext.getTenant().getId());
    }

}
