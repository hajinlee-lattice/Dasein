package com.latticeengines.propdata.engine.common.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;

public interface SourceColumnDao extends BaseDao<SourceColumn> {

    List<SourceColumn> getColumnsOfSource(String sourceName);
}
