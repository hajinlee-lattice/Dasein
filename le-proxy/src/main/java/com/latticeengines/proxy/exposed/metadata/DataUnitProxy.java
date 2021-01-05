package com.latticeengines.proxy.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitProxy {

    DataUnit create(String customerSpace, DataUnit dataUnit);

    DataUnit updateByNameAndType(String customerSpace, DataUnit dataUnit);

    List<DataUnit> getByStorageType(String customerSpace, DataUnit.StorageType type);

    DataUnit getByNameAndType(String customerSpace, String name, DataUnit.StorageType type);

    DataUnit getByDataTemplateIdAndRole(String customerSpace, String dataTemplateId, DataUnit.Role role);

    Boolean renameTableName(String customerSpace, DataUnit dataUnit, String tableName);

    Boolean delete(String customerSpace, DataUnit dataUnit);

    Boolean delete(String customerSpace, String name, DataUnit.StorageType type);

    void updateSignature(String customerSpace, DataUnit dataUnit, String signature);

    AthenaDataUnit registerAthenaDataUnit(String customerSpace, String name);

}
