package com.latticeengines.propdata.match.service.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.entitymanager.ExternalColumnEntityMgr;
import com.latticeengines.propdata.match.service.ExternalColumnService;

@Component("externalColumnService")
public class ExternalColumnServiceImpl implements ExternalColumnService {

    @Autowired
    private ExternalColumnEntityMgr externalColumnEntityMgr;

    @Autowired
    @Qualifier("pdScheduler")
    private ThreadPoolTaskScheduler scheduler;

    private ConcurrentMap<String, ExternalColumn> columnCache = new ConcurrentHashMap<>();

    @PostConstruct
    private void postConstruct() {
        loadCache();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCache();
            }
        }, TimeUnit.MINUTES.toMillis(1));
    }

    @Override
    public List<ExternalColumn> findByColumnSelection(ColumnSelection.Predefined selectName) {
        return externalColumnEntityMgr.findByTag(selectName.getName());
    }

    @Override
    public ExternalColumn getExternalColumn(String externalColumnId) {
        return columnCache.get(externalColumnId);
    }

    private void loadCache() {
        List<ExternalColumn> externalColumns = externalColumnEntityMgr.findAll();
        Set<String> toRemove = new HashSet<>(columnCache.keySet());
        for (ExternalColumn column: externalColumns) {
            columnCache.put(column.getExternalColumnID(), column);
            toRemove.remove(column.getExternalColumnID());
        }
        for (String colId: toRemove) {
            columnCache.remove(colId);
        }
    }

}
