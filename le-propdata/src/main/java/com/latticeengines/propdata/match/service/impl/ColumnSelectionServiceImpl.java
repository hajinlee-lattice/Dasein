package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.service.ColumnSelectionService;
import com.latticeengines.propdata.match.service.ExternalColumnService;

@Component
public class ColumnSelectionServiceImpl implements ColumnSelectionService {

    private Log log = LogFactory.getLog(ColumnSelectionServiceImpl.class);

    @Autowired
    private ExternalColumnService externalColumnService;

    private LoadingCache<ColumnSelection.Predefined, Map<String, List<String>>> sourceColumnMapCache;
    private LoadingCache<ColumnSelection.Predefined, Map<String, List<String>>> columnPriorityMapCache;

    @PostConstruct
    private void postConstruct() {
        buildSourceColumnMapCache();
        buildColumnPriorityMapCache();

        // warm up the caches
        try {
            getSourceColumnMap(ColumnSelection.Predefined.Model);
            getColumnPriorityMap(ColumnSelection.Predefined.Model);
            getSourceColumnMap(ColumnSelection.Predefined.DerivedColumns);
            getColumnPriorityMap(ColumnSelection.Predefined.DerivedColumns);
        } catch (Exception e) {
            log.error("Failed to preload caches using LDC_ManageDB", e);
        }
    }

    @Override
    public List<ColumnMetadata> getMetaData(ColumnSelection selection) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public List<String> getTargetColumns(ColumnSelection.Predefined predefined) {
        return getSourceColumnMap(predefined).get(predefined.getName());
    }

    @Override
    public Map<String, List<String>> getSourceColumnMap(ColumnSelection.Predefined predefined) {
        try {
            if (ColumnSelection.Predefined.Model.equals(predefined) ||
                    ColumnSelection.Predefined.DerivedColumns.equals(predefined)) {
                return sourceColumnMapCache.get(predefined);
            } else {
                throw new UnsupportedOperationException(
                        "Only support selection " + ColumnSelection.Predefined.Model
                                + " and " + ColumnSelection.Predefined.DerivedColumns + " now");
            }
        } catch (ExecutionException e) {
            log.warn("Failed to find sourceColumnMap for selection " + predefined + " in cache");
            return getSourceColumnMapForSelection(predefined);
        }
    }

    @Override
    public Map<String, List<String>> getColumnPriorityMap(ColumnSelection.Predefined predefined) {
        try {
            if (ColumnSelection.Predefined.Model.equals(predefined) ||
                    ColumnSelection.Predefined.DerivedColumns.equals(predefined)) {
                return columnPriorityMapCache.get(predefined);
            } else {
                throw new UnsupportedOperationException(
                        "Only support selections " + ColumnSelection.Predefined.Model
                                + " and " + ColumnSelection.Predefined.DerivedColumns + " now");
            }
        } catch (ExecutionException e) {
            log.warn("Failed to find columnPriorityMap for selection " + predefined + " in cache");
            return getColumnPriorityMapForSelection(predefined);
        }
    }

    private Map<String, List<String>> getSourceColumnMapForSelection(ColumnSelection.Predefined selection) {
        List<ExternalColumn> columns = externalColumnService.columnSelection(selection);
        Map<String, List<String>> map = new HashMap<>();
        List<String> columnNames = new ArrayList<>();
        for (ExternalColumn column : columns) {
            columnNames.add(column.getDefaultColumnName());
        }
        map.put(selection.getName(), columnNames);
        return map;
    }

    private Map<String, List<String>> getColumnPriorityMapForSelection(ColumnSelection.Predefined selection) {
        List<ExternalColumn> columns = externalColumnService.columnSelection(selection);
        Map<String, List<String>> map = new HashMap<>();
        for (ExternalColumn column : columns) {
            String columnName = column.getDefaultColumnName();
            List<String> sourceList = new ArrayList<>();
            sourceList.add(selection.getName());
            map.put(columnName, sourceList);
        }
        return map;
    }

    private void buildSourceColumnMapCache() {
        sourceColumnMapCache = CacheBuilder.newBuilder().concurrencyLevel(4).weakKeys()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<ColumnSelection.Predefined, Map<String, List<String>>>() {
                    public Map<String, List<String>> load(ColumnSelection.Predefined key) {
                        if (ColumnSelection.Predefined.Model.equals(key) ||
                                ColumnSelection.Predefined.DerivedColumns.equals(key)) {
                            return getSourceColumnMapForSelection(key);
                        } else {
                            return new HashMap<>();
                        }
                    }
                });
    }

    private void buildColumnPriorityMapCache() {
        columnPriorityMapCache = CacheBuilder.newBuilder().concurrencyLevel(4).weakKeys()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<ColumnSelection.Predefined, Map<String, List<String>>>() {
                    public Map<String, List<String>> load(ColumnSelection.Predefined key) {
                        if (ColumnSelection.Predefined.Model.equals(key) ||
                                ColumnSelection.Predefined.DerivedColumns.equals(key)) {
                            return getColumnPriorityMapForSelection(key);
                        } else {
                            return new HashMap<>();
                        }
                    }
                });
    }

}
