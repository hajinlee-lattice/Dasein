package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

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

    private ConcurrentMap<ColumnSelection.Predefined, Map<String, List<String>>> sourceColumnMapCache = new ConcurrentHashMap<>();
    private ConcurrentMap<ColumnSelection.Predefined, Map<String, List<String>>> columnPriorityMapCache = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("propdataScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @PostConstruct
    private void postConstruct() {
        loadCaches();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCaches();
            }
        }, TimeUnit.MINUTES.toMillis(1));
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
            if (ColumnSelection.Predefined.supportedSelections.contains(predefined)) {
                return sourceColumnMapCache.get(predefined);
            } else {
                throw new UnsupportedOperationException(
                        "Only support selections are " + ColumnSelection.Predefined.supportedSelections);
            }
        } catch (Exception e) {
            log.warn("Failed to find sourceColumnMap for selection " + predefined + " in cache");
            Map<String, List<String>> value = getSourceColumnMapForSelection(predefined);
            sourceColumnMapCache.put(predefined, value);
            return value;
        }
    }

    @Override
    public Map<String, List<String>> getColumnPriorityMap(ColumnSelection.Predefined predefined) {
        try {
            if (ColumnSelection.Predefined.supportedSelections.contains(predefined)) {
                return columnPriorityMapCache.get(predefined);
            } else {
                throw new UnsupportedOperationException(
                        "Only support selections are " + ColumnSelection.Predefined.supportedSelections);
            }
        } catch (Exception e) {
            log.warn("Failed to find columnPriorityMap for selection " + predefined + " in cache");
            Map<String, List<String>> value = getColumnPriorityMapForSelection(predefined);
            columnPriorityMapCache.put(predefined, value);
            return value;
        }
    }

    @Override
    public String getCurrentVersion(ColumnSelection.Predefined predefined) {
        return "1.0";
    }

    @Override
    public Boolean isValidVersion(ColumnSelection.Predefined predefined, String version) {
        return "1.0".equals(version);
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

    private void loadCaches() {
        for (ColumnSelection.Predefined selection : ColumnSelection.Predefined.supportedSelections) {
            try {
                sourceColumnMapCache.put(selection, getSourceColumnMapForSelection(selection));
            } catch (Exception e) {
                log.error(e);
            }
            try {
                columnPriorityMapCache.put(selection, getColumnPriorityMapForSelection(selection));
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

}
