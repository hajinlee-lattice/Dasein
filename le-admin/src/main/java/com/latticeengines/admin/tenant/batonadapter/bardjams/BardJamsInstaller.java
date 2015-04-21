package com.latticeengines.admin.tenant.batonadapter.bardjams;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;
import com.latticeengines.domain.exposed.admin.BardJamsTenantStatus;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class BardJamsInstaller extends LatticeComponentInstaller {

    private int timeout = 30000;

    private final Log log = LogFactory.getLog(this.getClass());

    private BardJamsEntityMgr bardJamsEntityMgr;

    public BardJamsInstaller() {
        super(BardJamsComponent.componentName);
    }

    @Override
    protected void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {

        BardJamsTenant tenant = pupulateTenant(space, serviceName, dataVersion, configDir);
        bardJamsEntityMgr.create(tenant);

        log.info("Created BardJams tenant=" + tenant.toString());

        boolean isSuccessful = checkTenant(tenant);
        if (isSuccessful) {
            log.info("Successfully created BardJams tenant=" + tenant);
        } else {
            log.info("Failed to create BardJams tenant=" + tenant);
            throw new LedpException(LedpCode.LEDP_18027);
        }

    }

    protected void setBardJamsEntityMgr(BardJamsEntityMgr bardJamsEntityMgr) {
        this.bardJamsEntityMgr = bardJamsEntityMgr;
    }

    protected void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    private boolean checkTenant(BardJamsTenant tenant) {
        long currTime = System.currentTimeMillis();
        long endTime = currTime + timeout;
        boolean isSuccessful = false;
        while (currTime < endTime) {
            log.info("Starting to check status of tenant=" + tenant.toString());
            BardJamsTenant newTenant = bardJamsEntityMgr.findByKey(tenant);
            if (newTenant.getStatus().equals(BardJamsTenantStatus.FINISHED.toString())) {
                isSuccessful = true;
                break;
            }
            if (newTenant.getStatus().equals(BardJamsTenantStatus.FAILED.toString())) {
                isSuccessful = false;
                break;
            }
            try {
                long wait_interval_mills = 3000L;
                Thread.sleep(wait_interval_mills);
            } catch (Exception ex) {
                log.warn("Warning!", ex);
            }
            currTime = System.currentTimeMillis();
        }

        return isSuccessful;
    }

    private BardJamsTenant pupulateTenant(CustomerSpace space, String serviceName, int dataVersion,
            DocumentDirectory configDir) {
        BardJamsTenant tenant = new BardJamsTenant();

        tenant.setTenant(getData(configDir, "Tenant"));
        tenant.setTenantType(getData(configDir, "TenantType"));
        tenant.setDlTenantName(getData(configDir, "DL_TenantName"));
        tenant.setDlUrl(getData(configDir, "DL_URL"));
        tenant.setDlUser(getData(configDir, "DL_User"));
        tenant.setDlPassword(getData(configDir, "DL_Password"));
        tenant.setNotificationEmail(getData(configDir, "NotificationEmail"));
        tenant.setNotifyEmailJob(getData(configDir, "NotifyEmailJob"));
        tenant.setJamsUser(getData(configDir, "JAMSUser"));
        tenant.setImmediateFolderStruct(getData(configDir, "ImmediateFolderStruct"));
        tenant.setScheduledFolderStruct(getData(configDir, "ScheduledFolderStruct"));
        tenant.setDanteManifestPath(getData(configDir, "DanteManifestPath"));
        tenant.setQueueName(getData(configDir, "Queue_Name"));
        tenant.setAgentName(getData(configDir, "Agent_Name"));
        tenant.setWeekdayScheduleName(getData(configDir, "WeekdaySchedule_Name"));
        tenant.setWeekendScheduleName(getData(configDir, "WeekendSchedule_Name"));
        tenant.setDataLaunchPath(getData(configDir, "Data_LaunchPath"));
        tenant.setDataArchivePath(getData(configDir, "Data_ArchivePath"));
        tenant.setDataLoaderToolsPath(getData(configDir, "DataLoaderTools_Path"));
        tenant.setDanteToolPath(getData(configDir, "DanteTool_Path"));
        String active = getData(configDir, "Active");
        if (active != null) {
            tenant.setActive(Integer.parseInt(active));
        }
        tenant.setDanteQueueName(getData(configDir, "Dante_Queue_Name"));
        tenant.setLoadGroupList(getData(configDir, "LoadGroupList"));
        tenant.setStatus(BardJamsTenantStatus.NEW.getStatus());

        return tenant;
    }

    private String getData(DocumentDirectory configDir, String field) {
        return configDir.get("/" + field).getDocument().getData();
    }
}
