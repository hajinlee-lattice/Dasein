package com.latticeengines.propdata.api.service.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.DomainFeatureTable;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.propdata.api.entitymanager.EntitlementEntityMgr;
import com.latticeengines.propdata.api.entitymanager.EntitlementSourceEntityMgr;
import com.latticeengines.propdata.api.service.EntitlementSourceService;

@Component("entitlementSourceService")
public class EntitlementSourceServiceImpl
        extends AbstractEntitlementService<EntitlementSourcePackages,
        EntitlementSourcePackageMap, EntitlementSourceContractPackageMap>
        implements EntitlementSourceService {

    @Autowired
    private EntitlementSourceEntityMgr entityMgr;

    @Override
    protected EntitlementEntityMgr<EntitlementSourcePackages,
            EntitlementSourcePackageMap, EntitlementSourceContractPackageMap>
    getEntityMgr() {
        return entityMgr;
    }

    @Override
    protected  EntitlementSourceContractPackageMap constructPackageContractMap(Long packageId, String contractId) {
        EntitlementSourcePackages entitlementPackage = entityMgr.getEntitlementPackage(packageId);
        if (entitlementPackage == null) {
            throw new IllegalStateException("There's no derived entitlement package with packageId=" + packageId + ".");
        }
        EntitlementSourceContractPackageMap ecpm = new EntitlementSourceContractPackageMap();
        ecpm.setContract_ID(contractId);
        ecpm.setEntitlementSourcePackage(entitlementPackage);
        ecpm.setSourcePackage_ID(entitlementPackage.getPid());
        ecpm.setLast_Modification_Date(new Date());
        return ecpm;
    }

    @Override
    protected EntitlementSourcePackages constructNewPackage(String packageName, String packageDescription, Boolean isDefault) {
        if(isDefault==null) isDefault=false;
        EntitlementSourcePackages entitlementPackage = new EntitlementSourcePackages();
        entitlementPackage.setSourcePackageName(packageName);
        entitlementPackage.setSourcePackageDescription(packageDescription);
        entitlementPackage.setIs_Default(isDefault);
        entitlementPackage.setLast_Modification_Date(new Date());
        return entitlementPackage;
    }

    @Override
    public List<EntitlementSourcePackageMap> getSourceEntitlementContents(Long packageId) {
        return entityMgr.getSourceEntitlementContents(packageId);
    }

    @Override
    public EntitlementSourcePackageMap addSourceToPackage(Long packageId, String lookupId) {
        EntitlementSourcePackages entitlementPackage = entityMgr.getEntitlementPackage(packageId);
        if (entitlementPackage == null) {
            throw new IllegalStateException("There's no EntitlementSourcePackages with packageId=" + packageId + ".");
        }

        Boolean isDomainBased;
        DomainFeatureTable dft = entityMgr.getDomainFeatureTable(lookupId);
        isDomainBased = (dft != null);

        EntitlementSourcePackageMap escpm = new EntitlementSourcePackageMap();
        escpm.setEntitlementSourcePackage(entitlementPackage);
        escpm.setSourcePackage_ID(entitlementPackage.getPid());
        escpm.setLookup_ID(lookupId);
        escpm.setLast_Modification_Date(new Date());
        escpm.setIsDomainBased(isDomainBased);
        return entityMgr.addPackageContentMap(escpm);
    }

    @Override
    public void removeSourceFromPackage(Long packageID, String lookupId) {
        EntitlementSourcePackageMap map = entityMgr.getSourcePackageMag(packageID, lookupId);
        if (map == null) {
            throw new IllegalStateException("There's no data column map with packageID=" + packageID
                    + " and lookupId=" + lookupId + ".");
        }
        entityMgr.removePackageContentMap(map);
    }
}
