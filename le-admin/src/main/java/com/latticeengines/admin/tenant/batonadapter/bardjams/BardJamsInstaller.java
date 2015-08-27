package com.latticeengines.admin.tenant.batonadapter.bardjams;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;
import com.latticeengines.domain.exposed.admin.BardJamsTenantStatus;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class BardJamsInstaller extends LatticeComponentInstaller {

    private static final long TIMEOUT = 180000L;
    private static final long WAIT_INTERVAL = 3000L;

    private final Log log = LogFactory.getLog(this.getClass());

    private BardJamsEntityMgr bardJamsEntityMgr;

    private TenantService tenantService;

    public BardJamsInstaller() {
        super(BardJamsComponent.componentName);
    }

    @Override
    protected DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {

        BardJamsTenant tenant = pupulateTenant(space, configDir);
        BardJamsTenant oldTenant = bardJamsEntityMgr.findByTenant(tenant);
        if (oldTenant == null) {
            bardJamsEntityMgr.create(tenant);
        } else {
            tenant.setPid(oldTenant.getPid());
            bardJamsEntityMgr.update(tenant);
        }

        log.info("Created BardJams tenant=" + tenant.toString());

        boolean isSuccessful = checkTenant(tenant);
        if (isSuccessful) {
            log.info("Successfully created BardJams tenant=" + tenant);
        } else {
            log.info("Failed to create BardJams tenant=" + tenant);
            throw new LedpException(LedpCode.LEDP_18027);
        }

        return configDir;

    }

    protected void setBardJamsEntityMgr(BardJamsEntityMgr bardJamsEntityMgr) {
        this.bardJamsEntityMgr = bardJamsEntityMgr;
    }

    protected void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }

    private boolean checkTenant(BardJamsTenant tenant) {
        long currTime = System.currentTimeMillis();
        long endTime = currTime + TIMEOUT;
        boolean isSuccessful = false;
        while (currTime < endTime) {
            log.info("Starting to check status of tenant=" + tenant.toString());
            BardJamsTenant newTenant = bardJamsEntityMgr.findByKey(tenant);
            String status = newTenant.getStatus().trim();
            if (BardJamsTenantStatus.FINISHED.getStatus().equals(status)) {
                isSuccessful = true;
                break;
            }
            if (BardJamsTenantStatus.FAILED.getStatus().equals(status)) {
                isSuccessful = false;
                break;
            }
            try {
                Thread.sleep(WAIT_INTERVAL);
            } catch (Exception ex) {
                log.warn("Warning!", ex);
            }
            currTime = System.currentTimeMillis();
        }
        BardJamsTenant newTenant = bardJamsEntityMgr.findByKey(tenant);
        String status = newTenant.getStatus().trim();
        if (BardJamsTenantStatus.NEW.getStatus().equals(status)) {
            Exception e = new IllegalStateException("The status of tenant " + tenant.getTenant()
                    + " remains NEW after " + String.valueOf(TIMEOUT /1000.) + " seconds.");
            throw new LedpException(LedpCode.LEDP_18027, e);
        }
        return isSuccessful;
    }

    private BardJamsTenant pupulateTenant(CustomerSpace space, DocumentDirectory configDir) {
        String contractId = space.getContractId();
        String tenantId = space.getTenantId();
        TenantDocument tenant = tenantService.getTenant(contractId, tenantId);
        DocumentDirectory vdbdlConfig = SerializableDocumentDirectory.deserialize(
                tenantService.getTenantServiceConfig(contractId, tenantId, VisiDBDLComponent.componentName));
        vdbdlConfig.makePathsLocal();
        return BardJamsComponent.getTenantFromDocDir(configDir, tenantId, tenant.getSpaceConfig(), vdbdlConfig);
    }
}
