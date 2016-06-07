package com.latticeengines.dellebi.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;

public interface DellEbiExecutionLogDao extends BaseDao<DellEbiExecutionLog> {

    DellEbiExecutionLog getEntryByFile(String file);

    List<DellEbiExecutionLog> getEntriesByFile(String file);

}
