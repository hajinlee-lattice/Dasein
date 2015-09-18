package com.latticeengines.propdata.api.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;

public interface EntitlementDerivedEntityMgr extends EntitlementEntityMgr<EntitlementPackages,
        EntitlementColumnMap, EntitlementContractPackageMap> {

    List<DataColumnMap> getDataColumns(Long packageId);

    DataColumnMap getDataColumn(String extensionName, String sourceTableName);

    EntitlementColumnMap getColumnMap(Long packageId, Long columnCalcId);
}
