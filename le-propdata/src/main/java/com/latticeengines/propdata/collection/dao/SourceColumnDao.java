package com.latticeengines.propdata.collection.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.collection.SourceColumn;
import com.latticeengines.propdata.collection.source.ServingSource;

public interface SourceColumnDao extends BaseDao<SourceColumn> {

    List<SourceColumn> getColumnsOfSource(ServingSource source);

}
