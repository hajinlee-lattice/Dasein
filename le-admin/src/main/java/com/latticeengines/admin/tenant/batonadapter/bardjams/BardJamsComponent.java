package com.latticeengines.admin.tenant.batonadapter.bardjams;

import java.util.Collections;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.tenant.batonadapter.LatticeComponent;
import com.latticeengines.admin.tenant.batonadapter.vdbdl.VisiDBDLComponent;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;
import com.latticeengines.domain.exposed.admin.BardJamsTenantStatus;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceInstaller;
import com.latticeengines.domain.exposed.camille.bootstrap.CustomerSpaceServiceUpgrader;

@Component
public class BardJamsComponent extends LatticeComponent {

    @Autowired
    private BardJamsEntityMgr bardJamsEntityMgr;

    @Autowired
    private VisiDBDLComponent visiDBDLComponent;

    @Autowired
    private TenantService tenantService;

    @Value("${admin.bardjams.timeout}")
    private int timeout;

    @Value("${admin.bardjams.dryrun}")
    private boolean dryrun;

    private LatticeComponentInstaller installer = new BardJamsInstaller();
    private CustomerSpaceServiceUpgrader upgrader = new BardJamsUpgrader();
    public static final String componentName = "BardJams";

    @PostConstruct
    public void setDependencies(){
        dependencies = Collections.singleton(visiDBDLComponent);
    }

    @Override
    public boolean doRegistration() {
        String defaultJson = "bardjams_default.json";
        String metadataJson = "bardjams_metadata.json";
        return uploadDefaultConfigAndSchemaByJson(defaultJson, metadataJson);
    }

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public void setName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CustomerSpaceServiceInstaller getInstaller() {

        ((BardJamsInstaller) installer).setBardJamsEntityMgr(bardJamsEntityMgr);
        ((BardJamsInstaller) installer).setTenantService(tenantService);
        ((BardJamsInstaller) installer).setTimeout(timeout);
        installer.setDryrun(dryrun);
        return installer;
    }

    @Override
    public CustomerSpaceServiceUpgrader getUpgrader() {
        return upgrader;
    }

    @Override
    public String getVersionString() {
        return "1.0";
    }

    public static BardJamsTenant getTenantFromDocDir(DocumentDirectory dir, String tenantId,
                                                     SpaceConfiguration spaceConfig, DocumentDirectory vdbdlConfig) {
        BardJamsTenant tenant = new BardJamsTenant();
        tenant.setTenant(tenantId);
        tenant.setTenantType(dir.getChild("TenantType").getDocument().getData());

        tenant.setDlTenantName(tenantId);
        tenant.setDlUrl(spaceConfig.getDlAddress());
        tenant.setDlUser(dir.getChild("DL_User").getDocument().getData());
        tenant.setDlPassword(dir.getChild("DL_Password").getDocument().getData());

        tenant.setNotificationEmail(dir.getChild("NotificationEmail").getDocument().getData());
        tenant.setNotifyEmailJob(dir.getChild("NotifyEmailJob").getDocument().getData());

        tenant.setJamsUser(dir.getChild("JAMSUser").getDocument().getData());

        tenant.setImmediateFolderStruct(dir.getChild("ImmediateFolderStruct").getDocument().getData());
        tenant.setScheduledFolderStruct(dir.getChild("ScheduledFolderStruct").getDocument().getData());
        tenant.setDanteManifestPath(dir.getChild("DanteManifestPath").getDocument().getData());

        tenant.setQueueName(dir.getChild("Queue_Name").getDocument().getData());
        tenant.setAgentName(dir.getChild("Agent_Name").getDocument().getData());

        tenant.setWeekdayScheduleName(dir.getChild("WeekdaySchedule_Name").getDocument().getData());
        tenant.setWeekendScheduleName(dir.getChild("WeekendSchedule_Name").getDocument().getData());

        tenant.setDataLaunchPath(dir.getChild("Data_LaunchPath").getDocument().getData());
        tenant.setDataArchivePath(dir.getChild("Data_ArchivePath").getDocument().getData());
        tenant.setDataLoaderToolsPath(dir.getChild("DataLoaderTools_Path").getDocument().getData());
        tenant.setDanteToolPath(dir.getChild("DanteTool_Path").getDocument().getData());

        String active = null;
        try {
            active = dir.getChild("Active").getDocument().getData();
        } catch (NullPointerException e) {
            // ignore
        }
        if (active != null) { tenant.setActive(Integer.parseInt(active)); }

        tenant.setDanteQueueName(dir.getChild("Dante_Queue_Name").getDocument().getData());
        tenant.setLoadGroupList(dir.getChild("LoadGroupList").getDocument().getData());

        tenant.setStatus(BardJamsTenantStatus.NEW.getStatus());
        return tenant;
    }

}
