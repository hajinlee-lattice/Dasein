package com.latticeengines.propdata.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public interface MetadataColumnService<E> {

    List<E> findByColumnSelection(ColumnSelection.Predefined selectName);

    E getMetadataColumn(String columnId);

    void loadCache();

}
