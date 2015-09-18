package com.latticeengines.propdata.api.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;

public interface EntitlementDerivedService extends EntitlementService<EntitlementPackages, EntitlementColumnMap,
        EntitlementContractPackageMap> {

    List<DataColumnMap> getDerivedColumns(Long packageId);

    EntitlementColumnMap addDerivedColumnToPackage(Long packageID, String extensionName, String sourceTableName);

    void removeDerivedColumnFromPackage(Long packageID, String extensionName, String sourceTableName);

}
