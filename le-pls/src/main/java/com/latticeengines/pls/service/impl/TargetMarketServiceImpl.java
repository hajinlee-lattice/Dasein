package com.latticeengines.pls.service.impl;

import java.rmi.server.UID;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketReportMap;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.latticeengines.workflow.exposed.entitymgr.ReportEntityMgr;

@Component("targetMarketService")
public class TargetMarketServiceImpl implements TargetMarketService {

    @SuppressWarnings("unused")
    private Log log = LogFactory.getLog(TargetMarketService.class);

    @Autowired
    private TargetMarketEntityMgr targetMarketEntityMgr;

    @Autowired
    private ReportEntityMgr reportEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataProxy metadataProxy;

    @Override
    public void createTargetMarket(TargetMarket targetMarket) {
        TargetMarket targetMarketStored = targetMarketEntityMgr.findTargetMarketByName(targetMarket.getName());
        if (targetMarketStored != null) {
            throw new LedpException(LedpCode.LEDP_18070, new String[] { targetMarket.getName() });
        }

        targetMarketEntityMgr.create(targetMarket);
    }

    @Override
    public void deleteTargetMarketByName(String name) {
        targetMarketEntityMgr.deleteTargetMarketByName(name);
    }

    @Override
    public TargetMarket findTargetMarketByName(String name) {
        return targetMarketEntityMgr.findTargetMarketByName(name);
    }

    @Override
    public List<TargetMarket> findAllTargetMarkets() {
        return targetMarketEntityMgr.findAllTargetMarkets();
    }

    @Override
    public void updateTargetMarketByName(TargetMarket targetMarket, String name) {
        targetMarketEntityMgr.updateTargetMarketByName(targetMarket, name);
    }

    @Override
    public TargetMarket createDefaultTargetMarket() {
        TargetMarket defaultTargetMarket = targetMarketEntityMgr.findTargetMarketByName(TargetMarket.DEFAULT_NAME);

        if (defaultTargetMarket == null) {
            defaultTargetMarket = targetMarketEntityMgr.createDefaultTargetMarket();
        }
        return defaultTargetMarket;
    }

    @Override
    public void registerReport(String targetMarketName, Report report) {
        TargetMarket targetMarket = findTargetMarketByName(targetMarketName);

        if (targetMarket == null) {
            throw new RuntimeException(String.format("No such target market with name %s", targetMarketName));
        }

        report.setName(new UID().toString());

        List<TargetMarketReportMap> reportMaps = targetMarket.getReports();
        Iterator<TargetMarketReportMap> iterator = reportMaps.iterator();
        while (iterator.hasNext()) {
            TargetMarketReportMap existingReport = iterator.next();
            if (existingReport.getReport().getPurpose().equals(report.getPurpose())) {
                iterator.remove();
            }
        }

        TargetMarketReportMap created = new TargetMarketReportMap();
        created.setReport(report);
        created.setTargetMarket(targetMarket);
        reportMaps.add(created);
        targetMarket.setReports(reportMaps);

        updateTargetMarketByName(targetMarket, targetMarketName);
    }

    @Override
    public Boolean resetDefaultTargetMarket() {
        CustomerSpace space = CustomerSpace.parse(SecurityContextUtils.getTenant().getId());
        Boolean result = metadataProxy.resetTables(space.toString());
        String dataTableHdfsPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), space).toString();
        try {
            if (result.equals(Boolean.TRUE)) {
                HdfsUtils.rmdir(yarnConfiguration, dataTableHdfsPath);
                return true;
            } else {
                throw new Exception("Reset Metadata Failed!");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
