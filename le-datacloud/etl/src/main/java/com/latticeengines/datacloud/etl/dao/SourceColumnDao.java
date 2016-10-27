package com.latticeengines.datacloud.etl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;

public interface SourceColumnDao extends BaseDao<SourceColumn> {

    List<SourceColumn> getColumnsOfSource(String sourceName);
}
