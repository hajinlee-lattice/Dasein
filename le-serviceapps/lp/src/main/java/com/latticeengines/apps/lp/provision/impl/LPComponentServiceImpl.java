package com.latticeengines.apps.lp.provision.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.provision.LPComponentManager;
import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.component.exposed.service.ComponentServiceBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.component.InstallDocument;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.metadata.service.MetadataService;

@Component("lpComponentService")
public class LPComponentServiceImpl extends ComponentServiceBase {

    private static final Logger log = LoggerFactory.getLogger(LPComponentServiceImpl.class);

    @Inject
    private LPComponentManager lpComponentManager;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataService metadataService;

    @Inject
    private SourceFileService sourceFileService;

    public LPComponentServiceImpl() {
        super(ComponentConstants.LP);
    }

    @Override
    public boolean install(String customerSpace, InstallDocument installDocument) {
        log.info("Start install LP component: " + customerSpace);
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            lpComponentManager.provisionTenant(cs, installDocument);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Provision LP service failed! " + e.toString());
            return false;
        }
        log.info(String.format("Install LP component: %s succeeded!", customerSpace));
        return true;
    }

    @Override
    public boolean update() {
        return false;
    }

    @Override
    public boolean destroy(String customerSpace) {
        log.info("Start uninstall LP component for: " + customerSpace);
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            lpComponentManager.discardTenant(cs.toString());
        } catch (Exception e) {
            log.error(String.format("Uninstall component LP for: %s failed. %s", customerSpace, e.toString()));
            return false;
        }
        return true;
    }

    @Override
    public boolean reset(String customerSpace) {
        log.info("Start reset LP component for: " + customerSpace);
        try {
            String podId = CamilleEnvironment.getPodId();
            String customerBase = "/user/s-analytics/customers";
            CustomerSpace cs = CustomerSpace.parse(customerSpace);

            String contractPath = PathBuilder.buildContractPath(podId, cs.getContractId()).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
                HdfsUtils.rmdir(yarnConfiguration, contractPath);
            }

            String customerPath = new Path(customerBase).append(cs.toString()).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, customerPath)) {
                HdfsUtils.rmdir(yarnConfiguration, customerPath);
            }

            contractPath = new Path(customerBase).append(cs.getContractId()).toString();
            if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
                HdfsUtils.rmdir(yarnConfiguration, contractPath);
            }

            log.info("Clean up METADATA_TABLE and METADATA_TABLE_xxx.");
            List<Table> tables = metadataService.getTables(cs);
            if (tables != null) {
                for (Table table : tables) {
                    metadataService.deleteTableAndCleanup(cs, table.getName());
                }
            }

            log.info("Clean up SourceFile");
            List<SourceFile> sourceFiles = sourceFileService.findAllSourceFiles();
            if (sourceFiles != null) {
                for (SourceFile sourceFile : sourceFiles) {
                    sourceFileService.delete(sourceFile.getName());
                }
            }

        } catch (Exception e) {
            log.error(String.format("Reset component LP for: %s failed. %s", customerSpace, e.toString()));
            return false;
        }
        return true;
    }
}
