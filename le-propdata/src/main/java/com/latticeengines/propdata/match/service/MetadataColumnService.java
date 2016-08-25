package com.latticeengines.propdata.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

public interface MetadataColumnService<E> {

    List<E> findByColumnSelection(Predefined selectName);

    E getMetadataColumn(String columnId);

    void loadCache();

}
