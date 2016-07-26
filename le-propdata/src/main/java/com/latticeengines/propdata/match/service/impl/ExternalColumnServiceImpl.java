package com.latticeengines.propdata.match.service.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.entitymanager.ExternalColumnEntityMgr;
import com.latticeengines.propdata.match.service.ExternalColumnService;

@Component("externalColumnService")
public class ExternalColumnServiceImpl implements ExternalColumnService {

    private static final Log log = LogFactory.getLog(ExternalColumnServiceImpl.class);

    @Autowired
    private ExternalColumnEntityMgr externalColumnEntityMgr;

    private final ConcurrentMap<String, ExternalColumn> whiteColumnCache = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<String> blackColumnCache = new ConcurrentSkipListSet<>();

    @PostConstruct
    private void postConstruct() {
        loadCache();
    }

    @Override
    public List<ExternalColumn> findByColumnSelection(ColumnSelection.Predefined selectName) {
        return externalColumnEntityMgr.findByTag(selectName.getName());
    }

    @Override
    public ExternalColumn getExternalColumn(String externalColumnId) {
        if (blackColumnCache.contains(externalColumnId)) {
            return null;
        }

        if (!whiteColumnCache.containsKey(externalColumnId)) {
            synchronized (whiteColumnCache) {
                ExternalColumn column;
                try {
                    column = externalColumnEntityMgr.findById(externalColumnId);
                } catch (Exception e) {
                    log.error(String.format("Failed to retrieve column information for [%s]", externalColumnId), e);
                    return null;
                }
                if (column == null) {
                    blackColumnCache.add(externalColumnId);
                    return null;
                } else {
                    whiteColumnCache.put(externalColumnId, column);
                    return column;
                }
            }
        } else {
            return whiteColumnCache.get(externalColumnId);
        }
    }

    @Override
    public void loadCache() {
        log.info("Start loading black and white column caches.");
        blackColumnCache.clear();
        whiteColumnCache.clear();
        List<ExternalColumn> externalColumns = externalColumnEntityMgr.findAll();
        synchronized (whiteColumnCache) {
            for (ExternalColumn column : externalColumns) {
                whiteColumnCache.put(column.getExternalColumnID(), column);
            }
        }
        log.info("Loaded " + whiteColumnCache.size() + " columns into white cache.");
    }

}
