package com.latticeengines.admin.tenant.batonadapter.vdbdl;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.admin.dynamicopts.impl.DataStoreProvider;
import com.latticeengines.admin.dynamicopts.impl.DataStoreProvider.DLFolder;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.camille.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.admin.CreateVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.GetVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.remote.exposed.service.DataLoaderService;

public class VisiDBDLInstaller extends LatticeComponentInstaller {

    private static final Log log = LogFactory.getLog(VisiDBDLInstaller.class);

    private TenantService tenantService;

    private DataStoreProvider dataStoreProvider;

    private DataLoaderService dataLoaderService;

    private static final int SUCCESS = 3;

    private static final int MASTER = 1;

    private static final int STANDALONE = 0;

    public VisiDBDLInstaller() {
        super(VisiDBDLComponent.componentName);
    }

    public void setTenantService(TenantService tenantService) {
        this.tenantService = tenantService;
    }

    public void setDataStoreProvider(DataStoreProvider dataStoreProvider) {
        this.dataStoreProvider = dataStoreProvider;
    }

    public void setDataloaderService(DataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }

    @Override
    public DocumentDirectory installComponentAndModifyConfigDir(CustomerSpace space, String serviceName,
                                                                int dataVersion, DocumentDirectory configDir) {
        String dmDeployment = space.getTenantId();
        String contractExternalID = space.getContractId();

        TenantDocument tenantDoc = tenantService.getTenant(contractExternalID, dmDeployment);
        String tenant = dmDeployment;
        String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();

        String tenantAlias = getData(configDir, "TenantAlias");
        String createNewVisiDB = getChild(configDir, "VisiDB", "CreateNewVisiDB").getDocument().getData();
        String caseSensitive = getChild(configDir, "VisiDB", "CaseSensitive").getDocument().getData();
        String visiDBName = getChild(configDir, "VisiDB", "VisiDBName").getDocument().getData();
        String visiDBServerName = getChild(configDir, "VisiDB", "ServerName").getDocument().getData();
        String visiDBFileDirectory = getChild(configDir, "VisiDB", "VisiDBFileDirectory").getDocument().getData();
        String cacheLimit = getChild(configDir, "VisiDB", "CacheLimit").getDocument().getData();
        String diskspaceLimit = getChild(configDir, "VisiDB", "DiskspaceLimit").getDocument().getData();
        String permanentStoreOption = getChild(configDir, "VisiDB", "PermanentStoreOption").getDocument().getData();
        String permanentStorePath = getChild(configDir, "VisiDB", "PermanentStore").getDocument().getData();
        String ownerEmail = getChild(configDir, "DL", "OwnerEmail").getDocument().getData();
        String dataStorePath = getChild(configDir, "DL", "DataStore").getDocument().getData();

        String dataStoreFolder = dataStoreProvider.toOptionKey(dataStorePath);
        dataStorePath = dataStorePath + "\\" + dmDeployment;
        permanentStorePath = permanentStorePath + "\\" + visiDBServerName.toUpperCase() + "\\" + tenant;

        if (StringUtils.isEmpty(tenantAlias)) {
            tenantAlias = tenant;
            configDir.getChild("TenantAlias").getDocument().setData(tenant);
        }
        if (StringUtils.isEmpty(visiDBName)) {
            visiDBName = tenant;
            getChild(configDir, "VisiDB", "VisiDBName").getDocument().setData(tenant);
        }

        String dataStoreLaunch = dataStorePath + "\\" + DLFolder.LAUNCH.toPath();
        getChild(configDir, "DL", "DataStore_Launch").getDocument() .setData(dataStoreLaunch);
        String dataStoreBackup = dataStorePath + "\\" + DLFolder.BACKUP.toPath();
        getChild(configDir, "DL", "DataStore_Backup").getDocument() .setData(dataStoreBackup);
        String dataStoreStatus = dataStorePath + "\\" + DLFolder.STATUS.toPath();
        getChild(configDir, "DL", "DataStore_Status").getDocument() .setData(dataStoreStatus);


        int permStoreOpt = MASTER;
        if (permanentStoreOption.equals("Master")) {
            permStoreOpt = MASTER;
        } else if (permanentStoreOption.equals("StandAlone")) {
            permStoreOpt = STANDALONE;
        }

        GetVisiDBDLRequest getRequest = new GetVisiDBDLRequest(tenant);
        try {
            InstallResult response = dataLoaderService.getDLTenantSettings(getRequest, dlUrl);
            int status = response.getStatus();
            String errorMessage = response.getErrorMessage();

            if (status != SUCCESS && !StringUtils.isEmpty(errorMessage) && errorMessage.contains("does not exist")) {
                createDataStoreFolder(dataStoreFolder, dmDeployment);
                CreateVisiDBDLRequest.Builder builder = new CreateVisiDBDLRequest.Builder(tenant, dmDeployment,
                        contractExternalID);
                builder.tenantAlias(tenantAlias) //
                        .ownerEmail(ownerEmail) //
                        .visiDBName(visiDBName) //
                        .visiDBLocation("ServerName=" + visiDBServerName) //
                        .visiDBFileDirectory(visiDBFileDirectory) //
                        .createNewVisiDB(Boolean.parseBoolean(createNewVisiDB)) //
                        .caseSensitive(Boolean.parseBoolean(caseSensitive)) //
                        .cacheLimit(Integer.parseInt(cacheLimit)) //
                        .diskspaceLimit(Integer.parseInt(diskspaceLimit)) //
                        .backupFolder(dataStoreBackup) //
                        .launchFolder(dataStoreLaunch) //
                        .launchStatusFolder(dataStoreStatus) //
                        .permanentStoreOption(permStoreOpt) //
                        .permanentStorePath(permanentStorePath);
                CreateVisiDBDLRequest postRequest = builder.build();
                response = dataLoaderService.createDLTenant(postRequest, dlUrl);
                status = response.getStatus();
                if (status != SUCCESS) {
                    throw new LedpException(LedpCode.LEDP_18032, new String[]{StringEscapeUtils.escapeJava(response.getErrorMessage())});
                }
                log.info("Tenant " + tenant + " has been successfully created in VisiDB/Dataloader");
            } else if (StringUtils.isEmpty(errorMessage) && status == SUCCESS) {
                log.warn("Tenant " + tenant + " has already been installed in VisiDB/Dataloader");
            } else {
                throw new LedpException(LedpCode.LEDP_18032, new String[] { StringEscapeUtils.escapeJava(errorMessage) });
            }
            return configDir;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18032, e);
        }
    }

    private void createDataStoreFolder(String dataStoreFolder, String dmDeployment) {
        dataStoreProvider.createTenantFolder(dataStoreFolder, dmDeployment);
    }
}
