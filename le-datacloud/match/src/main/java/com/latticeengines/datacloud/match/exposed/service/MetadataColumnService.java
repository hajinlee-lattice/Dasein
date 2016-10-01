package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.datacloud.match.service.HasDataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public interface MetadataColumnService<E extends MetadataColumn> extends HasDataCloudVersion {

    List<E> findByColumnSelection(Predefined selectName);

    E getMetadataColumn(String columnId);

    void loadCache();

}
