package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

public interface DataUnitService {

    DataUnit createOrUpdateByName(DataUnit dataUnit);

    DataUnit findByNameFromReader(String name);

    void deleteByName(String name);

}
