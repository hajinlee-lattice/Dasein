package com.latticeengines.apps.cdl.provision.impl;


import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DropBoxCrossTenantService;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.component.exposed.service.ComponentServiceBase;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.component.ComponentConstants;
import com.latticeengines.domain.exposed.component.InstallDocument;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.DataUnitCrossTenantService;
import com.latticeengines.metadata.service.DataUnitService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("cdlComponentService")
public class CDLComponentServiceImpl extends ComponentServiceBase {

    private static final Logger log = LoggerFactory.getLogger(CDLComponentServiceImpl.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private DataUnitService dataUnitService;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private SegmentService segmentService;

    @Inject
    private ActionService actionService;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private S3Service s3Service;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Inject
    private DataUnitCrossTenantService dataUnitCrossTenantService;

    @Inject
    private DropBoxCrossTenantService dropBoxCrossTenantService;

    @Value("${aws.customer.s3.bucket}")
    private String customersBucket;

    public CDLComponentServiceImpl() {
        super(ComponentConstants.CDL);
    }

    @Override
    public boolean install(String customerSpace, InstallDocument installDocument) {
        log.info("Start install CDL component: " + customerSpace);
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            Tenant tenant = tenantEntityMgr.findByTenantId(cs.toString());
            MultiTenantContext.setTenant(tenant);
            dataCollectionEntityMgr.createDefaultCollection();
            DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(cs.toString());
            log.info("Initialized data collection " + dataFeed.getDataCollection().getName());
            provisionDropBox(cs);
            s3ImportSystemService.createDefaultImportSystem(cs.toString());
            dropBoxService.createTenantDefaultFolder(cs.toString());
        } catch (Exception e) {
            log.error(String.format("Install CDL component for %s failed. %s", customerSpace, e.toString()));
            return false;
        }
        log.info(String.format("Install CDL component for %s succeeded.", customerSpace));
        return true;
    }

    @Override
    public boolean update() {
        return false;
    }

    @Override
    public boolean destroy(String customerSpace) {
        log.info("Start uninstall CDL component for: " + customerSpace);
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            String tenantId = cs.getTenantId();
            attrConfigEntityMgr.cleanupTenant(tenantId);
            dataUnitCrossTenantService.cleanupByTenant(customerSpace);
            dropBoxCrossTenantService.delete(customerSpace);
        } catch (Exception e) {
            log.error(String.format("Uninstall CDL component for: %s failed. %s", customerSpace, e.toString()));
            return false;
        }
        return true;
    }

    private void provisionDropBox(CustomerSpace customerSpace) {
        DropBox dropBox = dropBoxService.create();
        log.info("Created dropbox " + dropBox.getDropBox() + " for " + customerSpace.getTenantId());
    }

    @Override
    public boolean reset(String customerSpace) {
        log.info(String.format("Start reset CDL component for: %s.", customerSpace));
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            Tenant tenant = tenantEntityMgr.findByTenantId(cs.toString());
            MultiTenantContext.setTenant(tenant);

            DataFeed dataFeed = dataFeedService.getDefaultDataFeed(customerSpace);
            if (dataFeed != null) {
                // delete redshift tables
                List<DataUnit> dataUnits = dataUnitService.findAllByType(DataUnit.StorageType.Redshift);
                if (dataUnits != null) {
                    for (DataUnit dataUnit : dataUnits) {
                        redshiftService.dropTable(dataUnit.getName());
                    }
                }

                // delete s3
                destroy(customerSpace);
                s3Service.cleanupPrefix(customersBucket, cs.getContractId());

                log.info("Clean up DataCollection(DataFeed_xxx, MetadataSegment, RatingEngine, Play)");
                DataCollection dataCollection = dataCollectionEntityMgr.findDefaultCollection();
                if (dataCollection != null) {
                    dataCollectionEntityMgr.delete(dataCollection);
                }

                log.info("Clean up Action");
                List<Action> actions = actionService.findAll();
                if (actions != null) {
                    for (Action action : actions) {
                        actionService.delete(action.getPid());
                    }
                }

                log.info("Clean up ModelSummary");
                List<ModelSummary> modelSummaries = modelSummaryProxy.getAllForTenant(tenant.getName());
                if (modelSummaries != null) {
                    for (ModelSummary modelSummary : modelSummaries) {
                        modelSummaryProxy.deleteByModelId(cs.toString(), modelSummary.getId());
                    }
                }

                log.info("Clean up WorkflowJob");
                workflowProxy.deleteByTenantPid(customerSpace, tenant.getPid());

                Thread.sleep(1000);
                install(customerSpace, null);
            }
        } catch (Exception e) {
            log.error(String.format("Reset CDL component for: %s failed. %s", customerSpace, e.toString()));
            return false;
        }

        log.info(String.format("Reset CDL component for: %s succeed.", customerSpace));
        return true;
    }
}
