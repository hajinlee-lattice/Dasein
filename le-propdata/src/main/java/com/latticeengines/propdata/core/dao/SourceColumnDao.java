package com.latticeengines.propdata.core.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.propdata.core.source.ServingSource;

public interface SourceColumnDao extends BaseDao<SourceColumn> {

    List<SourceColumn> getColumnsOfSource(ServingSource source);

}
