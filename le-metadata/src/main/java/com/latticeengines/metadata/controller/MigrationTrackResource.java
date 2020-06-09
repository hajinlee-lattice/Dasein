package com.latticeengines.metadata.controller;


import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ImportMigrateReport;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.MigrationTrackEntityMgr;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

import io.swagger.annotations.Api;

@Api(value = "metadata")
@RestController
@RequestMapping("/migration")
public class MigrationTrackResource {

    @Inject
    private MigrationTrackEntityMgr migrationTrackEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private CDLProxy cdlProxy;

    @GetMapping
    @ResponseBody
    public List<MigrationTrack> getallTracks() {
        return migrationTrackEntityMgr.findAll();
    }

    @GetMapping("/tenants/getByStatus/{status}")
    @ResponseBody
    public List<Long> getTenantPidsByStatus(@PathVariable(name = "status") MigrationTrack.Status status) {
        return migrationTrackEntityMgr.getTenantPidsByStatus(status);
    }

    // get tenant active collection version in migration table
    @GetMapping("/tenants/{customerSpace}/activeDataCollection/version")
    @ResponseBody
    public DataCollection.Version getActiveDataCollectionVersion(@PathVariable(name = "customerSpace") String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new IllegalArgumentException(String.format("Tenant %s not found", customerSpace));
        }
        return migrationTrackEntityMgr.findByTenant(tenant).getDataCollection().getVersion();
    }

    @GetMapping("/tenants/{customerSpace}/tables/{tableName}/canDeleteOrRename")
    @ResponseBody
    public boolean canDeleteOrRenameTable(@PathVariable(name = "customerSpace") String customerSpace,
                                          @PathVariable(name = "tableName") String tableName) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new IllegalArgumentException(String.format("Tenant %s not found", customerSpace));
        }
        return migrationTrackEntityMgr.canDeleteOrRenameTable(tenant, tableName);
    }

    @GetMapping("/tenants/{customerSpace}/status")
    @ResponseBody
    public MigrationTrack.Status getMigrationStatus(@PathVariable(name = "customerSpace") String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new IllegalArgumentException(String.format("Tenant %s not found", customerSpace));
        }
        MigrationTrack track = migrationTrackEntityMgr.findByTenant(tenant);
        return track == null ? null : track.getStatus();
    }

    @GetMapping("/tenants/{customerSpace}/activeTables")
    @ResponseBody
    public Map<TableRoleInCollection, String[]> getActiveTables(@PathVariable(name = "customerSpace") String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new IllegalArgumentException(String.format("Tenant %s not found", customerSpace));
        }
        MigrationTrack track = migrationTrackEntityMgr.findByTenant(tenant);
        if (track == null) {
            throw new IllegalArgumentException(String.format("Tenant %s is not tracked for migration", customerSpace));
        }
        return track.getCurActiveTable();
    }

    @PutMapping("/tenants/{customerSpace}/importMigrateTracking")
    @ResponseBody
    public Boolean updateImportTracking(@PathVariable(name = "customerSpace") String customerSpace,
                                        @RequestBody ImportMigrateTracking importMigrateTracking) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new IllegalArgumentException(String.format("Tenant %s not found", customerSpace));
        }
        MigrationTrack track = migrationTrackEntityMgr.findByTenant((tenant));
        if (track == null) {
            throw new IllegalArgumentException(String.format("Tenant %s is not tracked for migration", customerSpace));
        }
        track.setImportMigrateTracking(importMigrateTracking);
        migrationTrackEntityMgr.update(track);
        return true;
    }

    @PostMapping("/tenants/{customerSpace}/restoreAllTemplates")
    @ResponseBody
    public Boolean restoreAllTemplates(@PathVariable(name = "customerSpace") String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        if (tenant == null) {
            throw new IllegalArgumentException(String.format("Tenant %s not found", customerSpace));
        }
        MigrationTrack track = migrationTrackEntityMgr.findByTenant((tenant));
        if (track == null) {
            throw new IllegalArgumentException(String.format("Tenant %s is not tracked for migration", customerSpace));
        }
        ImportMigrateTracking tracking = track.getImportMigrateTracking();
        if (tracking == null || tracking.getReport() == null || tracking.getReport().getBackupTemplateList() == null) {
            throw new IllegalArgumentException(String.format("Unable to find backup template record for tenant %s.", customerSpace));
        }
        List<ImportMigrateReport.BackupInfo> backupTemplateList = tracking.getReport().getBackupTemplateList();
        for (ImportMigrateReport.BackupInfo backupInfo : backupTemplateList) {
            cdlProxy.restoreTemplate(customerSpace, backupInfo.getTaskId(), backupInfo.getBackupName(), false);
        }
        return true;
    }
}
