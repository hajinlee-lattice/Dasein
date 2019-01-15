package com.latticeengines.proxy.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitProxy {

    DataUnit create(String customerSpace, DataUnit dataUnit);

    List<DataUnit> findAll(String customerSpace, DataUnit.StorageType type);

    DataUnit getByNameAndType(String customerSpace, String name, DataUnit.StorageType type);

}
