package com.latticeengines.dellebi.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.dellebi.DellEbiExecutionLog;

public interface DellEbiExecutionLogEntityMgr {

    void executeUpdate(DellEbiExecutionLog dellEbiExecutionLog);

    void create(DellEbiExecutionLog dellEbiExecutionLog);

    void createOrUpdate(DellEbiExecutionLog dellEbiExecutionLog);

    DellEbiExecutionLog getEntryByFile(String file);

    void recordFailure(DellEbiExecutionLog dellEbiExecutionLog, String err, int retryCount);

    void recordRetryFailure(DellEbiExecutionLog dellEbiExecutionLog, String err, int retryCount);

    List<DellEbiExecutionLog> getEntriesByFile(String file);

}
