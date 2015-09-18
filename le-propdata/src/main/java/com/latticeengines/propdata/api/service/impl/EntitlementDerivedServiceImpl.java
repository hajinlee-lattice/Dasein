package com.latticeengines.propdata.api.service.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.propdata.api.entitymanager.EntitlementDerivedEntityMgr;
import com.latticeengines.propdata.api.entitymanager.EntitlementEntityMgr;
import com.latticeengines.propdata.api.service.EntitlementDerivedService;

@Component("entitlementDerivedService")
public class EntitlementDerivedServiceImpl
        extends AbstractEntitlementService<EntitlementPackages, EntitlementColumnMap, EntitlementContractPackageMap>
        implements EntitlementDerivedService {

    @Autowired
    private EntitlementDerivedEntityMgr entityMgr;

    @Override
    protected EntitlementEntityMgr<EntitlementPackages, EntitlementColumnMap, EntitlementContractPackageMap>
    getEntityMgr() {
        return entityMgr;
    }

    @Override
    protected  EntitlementContractPackageMap constructPackageContractMap(Long packageId, String contractId) {
        EntitlementPackages entitlementPackage = entityMgr.getEntitlementPackage(packageId);
        if (entitlementPackage == null) {
            throw new IllegalStateException("There's no derived entitlement package with packageId=" + packageId + ".");
        }
        EntitlementContractPackageMap ecpm = new EntitlementContractPackageMap();
        ecpm.setContract_ID(contractId);
        ecpm.setEntitlementPackage(entitlementPackage);
        ecpm.setPackage_ID(entitlementPackage.getPid());
        ecpm.setLast_Modification_Date(new Date());
        return ecpm;
    }

    @Override
    protected EntitlementPackages constructNewPackage(String packageName, String packageDescription, Boolean isDefault) {
        if(isDefault==null) isDefault=false;
        EntitlementPackages entitlementPackage = new EntitlementPackages();
        entitlementPackage.setPackageName(packageName);
        entitlementPackage.setPackageDescription(packageDescription);
        entitlementPackage.setIs_Default(isDefault);
        entitlementPackage.setLast_Modification_Date(new Date());
        return entitlementPackage;
    }

    @Override
    public List<DataColumnMap> getDerivedColumns(Long packageId) {
        return entityMgr.getDataColumns(packageId);
    }

    @Override
    public EntitlementColumnMap addDerivedColumnToPackage(Long packageId, String extensionName, String sourceTableName) {
        DataColumnMap dc = entityMgr.getDataColumn(extensionName, sourceTableName);
        if (dc == null) {
            throw new IllegalStateException("There's no data column map with extensionName=" + extensionName
                    + " and sourceTableName=" + sourceTableName + ".");
        }
        EntitlementPackages entitlementPackage = entityMgr.getEntitlementPackage(packageId);
        if (entitlementPackage == null) {
            throw new IllegalStateException("There's no derived entitlement package with packageId=" + packageId + ".");
        }
        EntitlementColumnMap ecm = new EntitlementColumnMap();
        ecm.setColumnCalculation_ID(dc.getColumnCalcID());
        ecm.setEntitlementPackage(entitlementPackage);
        ecm.setPackage_ID(entitlementPackage.getPid());
        ecm.setLast_Modification_Date(new Date());
        return entityMgr.addPackageContentMap(ecm);
    }

    @Override
    public void removeDerivedColumnFromPackage(Long packageID, String extensionName, String sourceTableName) {
        DataColumnMap dc = entityMgr.getDataColumn(extensionName, sourceTableName);
        if (dc == null) {
            throw new IllegalStateException("There's no data column map with extensionName=" + extensionName
                    + " and sourceTableName=" + sourceTableName + ".");
        }
        EntitlementColumnMap ecm = entityMgr.getColumnMap(packageID, dc.getColumnCalcID());
        if (ecm == null) {
            throw new IllegalStateException("Cannot find requested EntitlementColumnMap.");
        }
        entityMgr.removePackageContentMap(ecm);
    }
}
