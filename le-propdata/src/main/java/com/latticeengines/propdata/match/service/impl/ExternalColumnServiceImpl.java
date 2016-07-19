package com.latticeengines.propdata.match.service.impl;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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

    private LoadingCache<String, ExternalColumn> columnCache;

    @PostConstruct
    private void postConstruct() {
        columnCache = CacheBuilder.newBuilder() //
                .concurrencyLevel(1024) //
                .maximumSize(2000) //
                .expireAfterWrite(48, TimeUnit.HOURS) //
                .build(new CacheLoader<String, ExternalColumn>() {
                    public ExternalColumn load(String key) {
                        return externalColumnEntityMgr.findById(key);
                    }
                });
        loadCache();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCache();
            }
        }, TimeUnit.MINUTES.toMillis(30));
    }

    @Override
    public List<ExternalColumn> findByColumnSelection(ColumnSelection.Predefined selectName) {
        return externalColumnEntityMgr.findByTag(selectName.getName());
    }

    @Override
    public ExternalColumn getExternalColumn(String externalColumnId) {
        try {
            return columnCache.get(externalColumnId);
        } catch (ExecutionException e) {
            throw new RuntimeException(
                    String.format("Failed to retrieve column information for [%s]", externalColumnId), e);
        }
    }

    private void loadCache() {
        List<ExternalColumn> externalColumns = findByColumnSelection(ColumnSelection.Predefined.RTS);
        for (ExternalColumn column : externalColumns) {
            columnCache.put(column.getExternalColumnID(), column);
        }
    }

}
