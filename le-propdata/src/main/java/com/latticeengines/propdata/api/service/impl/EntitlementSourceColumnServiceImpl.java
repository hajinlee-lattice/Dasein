package com.latticeengines.propdata.api.service.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.DomainFeatureTable;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.propdata.api.entitymanager.EntitlementEntityMgr;
import com.latticeengines.propdata.api.entitymanager.EntitlementSourceColumnEntityMgr;
import com.latticeengines.propdata.api.service.EntitlementSourceColumnService;

@Component("entitlementSourceColumnService")
public class EntitlementSourceColumnServiceImpl
        extends AbstractEntitlementService<EntitlementSourceColumnsPackages,
        EntitlementSourceColumnsPackageMap, EntitlementSourceColumnsContractPackageMap>
        implements EntitlementSourceColumnService {

    @Autowired
    private EntitlementSourceColumnEntityMgr entityMgr;

    @Override
    protected EntitlementEntityMgr<EntitlementSourceColumnsPackages,
            EntitlementSourceColumnsPackageMap, EntitlementSourceColumnsContractPackageMap>
    getEntityMgr() {
        return entityMgr;
    }

    @Override
    protected  EntitlementSourceColumnsContractPackageMap constructPackageContractMap(Long packageId, String contractId) {
        EntitlementSourceColumnsPackages entitlementPackage = entityMgr.getEntitlementPackage(packageId);
        if (entitlementPackage == null) {
            throw new IllegalStateException("There's no source column entitlement package with packageId=" + packageId + ".");
        }
        EntitlementSourceColumnsContractPackageMap escpm = new EntitlementSourceColumnsContractPackageMap();
        escpm.setContract_ID(contractId);
        escpm.setEntitlementSourceColumnsPackage(entitlementPackage);
        escpm.setSourceColumnsPackage_ID(entitlementPackage.getPid());
        escpm.setLast_Modification_Date(new Date());
        return escpm;
    }

    @Override
    protected EntitlementSourceColumnsPackages constructNewPackage(String packageName, String packageDescription, Boolean isDefault) {
        if(isDefault==null) isDefault=false;
        EntitlementSourceColumnsPackages entitlementPackage = new EntitlementSourceColumnsPackages();
        entitlementPackage.setSourceColumnsPackageName(packageName);
        entitlementPackage.setSourceColumnsPackageDescription(packageDescription);
        entitlementPackage.setIs_Default(isDefault);
        entitlementPackage.setLast_Modification_Date(new Date());
        return entitlementPackage;
    }

    @Override
    public List<EntitlementSourceColumnsPackageMap> getPackageSourceColumns(Long packageId) {
        return entityMgr.getPackageSourceColumns(packageId);
    }

    @Override
    public EntitlementSourceColumnsPackageMap addColumnToPackage(Long packageId, String lookupId, String columnName) {
        EntitlementSourceColumnsPackages entitlementPackage = entityMgr.getEntitlementPackage(packageId);
        if (entitlementPackage == null) {
            throw new IllegalStateException("There's no source column package with packageId=" + packageId + ".");
        }

        DomainFeatureTable dft = entityMgr.getDomainFeatureTable(lookupId);
        Boolean isDomainBased = (dft != null);

        EntitlementSourceColumnsPackageMap escpm = new EntitlementSourceColumnsPackageMap();
        escpm.setColumnName(columnName);
        escpm.setLookup_ID(lookupId);
        escpm.setEntitlementSourceColumnsPackage(entitlementPackage);
        escpm.setSourceColumnsPackage_ID(entitlementPackage.getPid());
        escpm.setLast_Modification_Date(new Date());
        escpm.setIsDomainBased(isDomainBased);
        return entityMgr.addPackageContentMap(escpm);
    }

    @Override
    public void removeColumnFromPackage(Long packageID, String lookupId, String columnName) {
        EntitlementSourceColumnsPackageMap map = entityMgr.getSourceColumnPackageMag(packageID, lookupId, columnName);
        if (map == null) {
            throw new IllegalStateException("There's no data column map with packageID=" + packageID
                    + " and lookupId=" + lookupId + " columnName=" + columnName +".");
        }
        entityMgr.removePackageContentMap(map);
    }
}
