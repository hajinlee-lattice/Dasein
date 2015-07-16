package com.latticeengines.domain.exposed.admin;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "Tenants")
public class BardJamsTenant implements HasPid {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "TenantID", unique = true, nullable = false)
    private Long tenantId;
    @Column(name = "Tenant", nullable = false)
    private String tenant;
    @Column(name = "TenantType", nullable = true)
    private String tenantType;
    @Column(name = "DL_TenantName", nullable = true)
    private String dlTenantName;
    @Column(name = "DL_URL", nullable = true)
    private String dlUrl;
    @Column(name = "DL_User", nullable = true)
    private String dlUser;
    @Column(name = "DL_Password", nullable = true)
    private String dlPassword;
    @Column(name = "AuthoritativeDB_Server", nullable = true)
    private String authoritativeDBServer;
    @Column(name = "LEDeployment_Ext_ID", nullable = true)
    private String leDeploymentExtID;
    @Column(name = "NotificationEmail", nullable = true)
    private String notificationEmail;
    @Column(name = "NotifyEmailJob", nullable = true)
    private String notifyEmailJob;
    @Column(name = "JAMSUser", nullable = true)
    private String jamsUser;
    @Column(name = "ImmediateFolderStruct", nullable = true)
    private String immediateFolderStruct;
    @Column(name = "ScheduledFolderStruct", nullable = true)
    private String scheduledFolderStruct;
    @Column(name = "ScheduledRefreshFolder", nullable = true)
    private String scheduledRefreshFolder;
    @Column(name = "DanteManifestPath", nullable = true)
    private String danteManifestPath;
    @Column(name = "Queue_Name", nullable = true)
    private String queueName;
    @Column(name = "Agent_Name", nullable = true)
    private String agentName;
    @Column(name = "RecoverJob", nullable = true)
    private String recoverJob;
    @Column(name = "WeekdaySchedule_Name", nullable = true)
    private String weekdayScheduleName;
    @Column(name = "WeekendSchedule_Name", nullable = true)
    private String weekendScheduleName;
    @Column(name = "Data_LaunchPath", nullable = true)
    private String dataLaunchPath;
    @Column(name = "Data_AlternateManifestPath", nullable = true)
    private String dataAlternativeManifestPath;
    @Column(name = "Data_ArchivePath", nullable = true)
    private String dataArchivePath;
    @Column(name = "salesPrismDB_Server", nullable = true)
    private String salesPrismDBServer;
    @Column(name = "salesPrismDB_Name", nullable = true)
    private String salesPrismDBName;
    @Column(name = "salesPrismDB_BackupPath", nullable = true)
    private String salesPrismDBBackupPath;
    @Column(name = "playMakerDB_Server", nullable = true)
    private String playMakerDBServer;
    @Column(name = "playMakerDB_Name", nullable = true)
    private String playMakerDBName;
    @Column(name = "AnalyticsDB_Server", nullable = true)
    private String analyticsDBServer;
    @Column(name = "AnalyticsDB_Name", nullable = true)
    private String analyticsDBName;
    @Column(name = "PoetAdminTool_Path", nullable = true)
    private String poetAdminToolPath;
    @Column(name = "DataLoaderTools_Path", nullable = true)
    private String dataLoaderToolsPath;
    @Column(name = "DanteTool_Path", nullable = true)
    private String danteToolPath;
    @Column(name = "AppPoolReset_Path", nullable = true)
    private String appPoolResetPath;
    @Column(name = "AppPoolReset_FileName", nullable = true)
    private String appPoolResetFileName;
    @Column(name = "LEAF_ProcsDB_Server", nullable = true)
    private String leafProcsDBServer;
    @Column(name = "LEAF_ProcsDB_Name", nullable = true)
    private String leafProcsDBName;
    @Column(name = "LEAF_Deployment", nullable = true)
    private String leafDeployment;
    @Column(name = "LEAF_Version", nullable = true)
    private String leafVersion;;
    @Column(name = "Active", nullable = true)
    private int active;
    @Column(name = "Dante_Queue_Name", nullable = true)
    private String danteQueueName;
    @Column(name = "loadGroupList", nullable = true)
    private String LoadGroupList;
    @Column(name = "Status", nullable = true)
    private String status;

    public BardJamsTenant() {
        super();
    }

    @Override
    public Long getPid() {
        return tenantId;
    }

    @Override
    public void setPid(Long id) {
        this.tenantId = id;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getTenantType() {
        return tenantType;
    }

    public void setTenantType(String tenantType) {
        this.tenantType = tenantType;
    }

    public String getDlTenantName() {
        return dlTenantName;
    }

    public void setDlTenantName(String dlTenantName) {
        this.dlTenantName = dlTenantName;
    }

    public String getDlUrl() {
        return dlUrl;
    }

    public void setDlUrl(String dlUrl) {
        this.dlUrl = dlUrl;
    }

    public String getDlUser() {
        return dlUser;
    }

    public void setDlUser(String dlUser) {
        this.dlUser = dlUser;
    }

    public String getDlPassword() {
        return dlPassword;
    }

    public void setDlPassword(String dlPassword) {
        this.dlPassword = dlPassword;
    }

    public String getAuthoritativeDBServer() {
        return authoritativeDBServer;
    }

    public void setAuthoritativeDBServer(String authoritativeDBServer) {
        this.authoritativeDBServer = authoritativeDBServer;
    }

    public String getLeDeploymentExtID() {
        return leDeploymentExtID;
    }

    public void setLeDeploymentExtID(String leDeploymentExtID) {
        this.leDeploymentExtID = leDeploymentExtID;
    }

    public String getNotificationEmail() {
        return notificationEmail;
    }

    public void setNotificationEmail(String notificationEmail) {
        this.notificationEmail = notificationEmail;
    }

    public String getNotifyEmailJob() {
        return notifyEmailJob;
    }

    public void setNotifyEmailJob(String notifyEmailJob) {
        this.notifyEmailJob = notifyEmailJob;
    }

    public String getJamsUser() {
        return jamsUser;
    }

    public void setJamsUser(String jamsUser) {
        this.jamsUser = jamsUser;
    }

    public String getImmediateFolderStruct() {
        return immediateFolderStruct;
    }

    public void setImmediateFolderStruct(String immediateFolderStruct) {
        this.immediateFolderStruct = immediateFolderStruct;
    }

    public String getScheduledFolderStruct() {
        return scheduledFolderStruct;
    }

    public void setScheduledFolderStruct(String scheduledFolderStruct) {
        this.scheduledFolderStruct = scheduledFolderStruct;
    }

    public String getScheduledRefreshFolder() {
        return scheduledRefreshFolder;
    }

    public void setScheduledRefreshFolder(String scheduledRefreshFolder) {
        this.scheduledRefreshFolder = scheduledRefreshFolder;
    }

    public String getDanteManifestPath() {
        return danteManifestPath;
    }

    public void setDanteManifestPath(String danteManifestPath) {
        this.danteManifestPath = danteManifestPath;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getAgentName() {
        return agentName;
    }

    public void setAgentName(String agentName) {
        this.agentName = agentName;
    }

    public String getRecoverJob() {
        return recoverJob;
    }

    public void setRecoverJob(String recoverJob) {
        this.recoverJob = recoverJob;
    }

    public String getWeekdayScheduleName() {
        return weekdayScheduleName;
    }

    public void setWeekdayScheduleName(String weekdayScheduleName) {
        this.weekdayScheduleName = weekdayScheduleName;
    }

    public String getWeekendScheduleName() {
        return weekendScheduleName;
    }

    public void setWeekendScheduleName(String weekendScheduleName) {
        this.weekendScheduleName = weekendScheduleName;
    }

    public String getDataLaunchPath() {
        return dataLaunchPath;
    }

    public void setDataLaunchPath(String dataLaunchPath) {
        this.dataLaunchPath = dataLaunchPath;
    }

    public String getDataAlternativeManifestPath() {
        return dataAlternativeManifestPath;
    }

    public void setDataAlternativeManifestPath(String dataAlternativeManifestPath) {
        this.dataAlternativeManifestPath = dataAlternativeManifestPath;
    }

    public String getDataArchivePath() {
        return dataArchivePath;
    }

    public void setDataArchivePath(String dataArchivePath) {
        this.dataArchivePath = dataArchivePath;
    }

    public String getSalesPrismDBServer() {
        return salesPrismDBServer;
    }

    public void setSalesPrismDBServer(String salesPrismDBServer) {
        this.salesPrismDBServer = salesPrismDBServer;
    }

    public String getSalesPrismDBName() {
        return salesPrismDBName;
    }

    public void setSalesPrismDBName(String salesPrismDBName) {
        this.salesPrismDBName = salesPrismDBName;
    }

    public String getSalesPrismDBBackupPath() {
        return salesPrismDBBackupPath;
    }

    public void setSalesPrismDBBackupPath(String salesPrismDBBackupPath) {
        this.salesPrismDBBackupPath = salesPrismDBBackupPath;
    }

    public String getPlayMakerDBServer() {
        return playMakerDBServer;
    }

    public void setPlayMakerDBServer(String playMakerDBServer) {
        this.playMakerDBServer = playMakerDBServer;
    }

    public String getPlayMakerDBName() {
        return playMakerDBName;
    }

    public void setPlayMakerDBName(String playMakerDBName) {
        this.playMakerDBName = playMakerDBName;
    }

    public String getAnalyticsDBServer() {
        return analyticsDBServer;
    }

    public void setAnalyticsDBServer(String analyticsDBServer) {
        this.analyticsDBServer = analyticsDBServer;
    }

    public String getAnalyticsDBName() {
        return analyticsDBName;
    }

    public void setAnalyticsDBName(String analyticsDBName) {
        this.analyticsDBName = analyticsDBName;
    }

    public String getPoetAdminToolPath() {
        return poetAdminToolPath;
    }

    public void setPoetAdminToolPath(String poetAdminToolPath) {
        this.poetAdminToolPath = poetAdminToolPath;
    }

    public String getDataLoaderToolsPath() {
        return dataLoaderToolsPath;
    }

    public void setDataLoaderToolsPath(String dataLoaderToolsPath) {
        this.dataLoaderToolsPath = dataLoaderToolsPath;
    }

    public String getDanteToolPath() {
        return danteToolPath;
    }

    public void setDanteToolPath(String danteToolPath) {
        this.danteToolPath = danteToolPath;
    }

    public String getAppPoolResetPath() {
        return appPoolResetPath;
    }

    public void setAppPoolResetPath(String appPoolResetPath) {
        this.appPoolResetPath = appPoolResetPath;
    }

    public String getAppPoolResetFileName() {
        return appPoolResetFileName;
    }

    public void setAppPoolResetFileName(String appPoolResetFileName) {
        this.appPoolResetFileName = appPoolResetFileName;
    }

    public String getLeafProcsDBServer() {
        return leafProcsDBServer;
    }

    public void setLeafProcsDBServer(String leafProcsDBServer) {
        this.leafProcsDBServer = leafProcsDBServer;
    }

    public String getLeafProcsDBName() {
        return leafProcsDBName;
    }

    public void setLeafProcsDBName(String leafProcsDBName) {
        this.leafProcsDBName = leafProcsDBName;
    }

    public String getLeafDeployment() {
        return leafDeployment;
    }

    public void setLeafDeployment(String leafDeployment) {
        this.leafDeployment = leafDeployment;
    }

    public String getLeafVersion() {
        return leafVersion;
    }

    public void setLeafVersion(String leafVersion) {
        this.leafVersion = leafVersion;
    }

    public int getActive() {
        return active;
    }

    public void setActive(int active) {
        this.active = active;
    }

    public String getDanteQueueName() {
        return danteQueueName;
    }

    public void setDanteQueueName(String danteQueueName) {
        this.danteQueueName = danteQueueName;
    }

    public String getLoadGroupList() {
        return LoadGroupList;
    }

    public void setLoadGroupList(String loadGroupList) {
        LoadGroupList = loadGroupList;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (!other.getClass().equals(this.getClass())) {
            return false;
        }
        BardJamsTenant theOther = (BardJamsTenant) other;
        return new EqualsBuilder().append(tenantId, theOther.getPid()).isEquals();
    }

    @Override
    public String toString() {
        return "BardJamsTenants="
                + new ToStringBuilder(tenantId).append(tenantId).append(tenant).append(status).toString();
    }
}
