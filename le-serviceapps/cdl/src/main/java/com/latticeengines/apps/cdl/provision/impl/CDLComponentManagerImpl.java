package com.latticeengines.apps.cdl.provision.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.provision.CDLComponentManager;
import com.latticeengines.apps.cdl.service.AtlasSchedulingService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DropBoxCrossTenantService;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.DataUnitCrossTenantService;

@Component
public class CDLComponentManagerImpl implements CDLComponentManager {

    private static final Logger log = LoggerFactory.getLogger(CDLComponentManagerImpl.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private DataUnitCrossTenantService dataUnitCrossTenantService;

    @Inject
    private DropBoxCrossTenantService dropBoxCrossTenantService;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Inject
    private AtlasSchedulingService atlasSchedulingService;

    @Inject
    private BatonService batonService;

    @Override
    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        String camilleContractId = space.getContractId();
        String camilleSpaceId = space.getSpaceId();
        String customerSpace = String.format("%s.%s.%s", camilleContractId, camilleTenantId, camilleSpaceId);
        log.info(String.format("Provisioning tenant %s", customerSpace));
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        MultiTenantContext.setTenant(tenant);
        dataCollectionEntityMgr.createDefaultCollection();
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        log.info("Initialized data collection " + dataFeed.getDataCollection().getName());
        provisionDropBox(space);

        List<LatticeProduct> products = getProducts(customerSpace);
        if (!products.contains(LatticeProduct.DCP)) {
            s3ImportSystemService.createDefaultImportSystem(space.toString());
            dropBoxService.createTenantDefaultFolder(space.toString());
            if (configDir.get("/ExportCronExpression") != null) {
                String exportCron = configDir.get("/ExportCronExpression").getDocument().getData();
                log.info(String.format("Export Cron for tenant %s is: %s", customerSpace, exportCron));
                atlasSchedulingService.createOrUpdateExportScheduling(customerSpace, exportCron);
            }
        }
    }

    @Override
    public void discardTenant(String customerSpace) {
        String tenantId = CustomerSpace.parse(customerSpace).getTenantId();
        attrConfigEntityMgr.cleanupTenant(tenantId);
        dataUnitCrossTenantService.cleanupByTenant(customerSpace);
        dropBoxCrossTenantService.delete(customerSpace);
    }

    private void provisionDropBox(CustomerSpace customerSpace) {
        DropBox dropBox = dropBoxService.create();
        log.info("Created dropbox " + dropBox.getDropBox() + " for " + customerSpace.getTenantId());
    }

    private List<LatticeProduct> getProducts(String customerSpace) {
        try {
            SpaceConfiguration spaceConfiguration = getSpaceConfiguration(customerSpace);
            return spaceConfiguration.getProducts();
        } catch (Exception e) {
            log.error("Failed to get product list of tenant " + customerSpace, e);
            return new ArrayList<>();
        }
    }

    private SpaceConfiguration getSpaceConfiguration(String tenantId) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            TenantDocument tenantDocument = batonService.getTenant(customerSpace.getContractId(),
                    customerSpace.getTenantId());
            return tenantDocument.getSpaceConfig();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18086, e, new String[] { tenantId });
        }
    }
}
