package com.latticeengines.apps.cdl.provision.impl;


import java.util.List;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
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
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("cdlComponentService")
public class CDLComponentServiceImpl extends ComponentServiceBase {

    private static final Logger log = LoggerFactory.getLogger(CDLComponentServiceImpl.class);

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private DataUnitEntityMgr dataUnitEntityMgr;

    @Inject
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private RedshiftService redshiftService;

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
            dataUnitEntityMgr.cleanupTenant(tenantId);
            Tenant tenant = tenantEntityMgr.findByTenantId(cs.toString());
            MultiTenantContext.setTenant(tenant);
            dropBoxService.delete();
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
        log.info("Start reset CDL component for: " + customerSpace);
        try {
            CustomerSpace cs = CustomerSpace.parse(customerSpace);
            Tenant tenant = tenantEntityMgr.findByTenantId(cs.toString());
            MultiTenantContext.setTenant(tenant);

            DataFeed dataFeed = dataFeedService.getDefaultDataFeed(customerSpace);
            if (dataFeed != null) {
                // delete redshift tables
                List<DataUnit> dataUnits = dataUnitEntityMgr.findAllByTypeFromReader(
                        customerSpace, DataUnit.StorageType.Redshift);
                if (dataUnits != null) {
                    for (DataUnit dataUnit : dataUnits) {
                        redshiftService.dropTable(dataUnit.getName());
                    }
                }

                // delete s3
                destroy(customerSpace);

                DataCollection dataCollection = dataCollectionEntityMgr.findDefaultCollection();
                if (dataCollection != null) {
                    dataCollectionEntityMgr.delete(dataCollection);
                }

                Thread.sleep(1000);
                install(customerSpace, null);
            }
        } catch (Exception e) {
            log.error(String.format("Reset CDL component for: %s failed. %s", customerSpace, e.toString()));
            return false;
        }
        return true;
    }
}
