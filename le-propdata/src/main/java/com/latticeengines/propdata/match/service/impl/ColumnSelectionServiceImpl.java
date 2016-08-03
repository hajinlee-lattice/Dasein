package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.service.ColumnSelectionService;
import com.latticeengines.propdata.match.service.MetadataColumnService;

@Component
public class ColumnSelectionServiceImpl implements ColumnSelectionService {

    private Log log = LogFactory.getLog(ColumnSelectionServiceImpl.class);

    @Resource(name = "externalColumnService")
    private MetadataColumnService<ExternalColumn> externalColumnService;

    private ConcurrentMap<ColumnSelection.Predefined, ColumnSelection> predefinedSelectionMap = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("pdScheduler")
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
    public ColumnSelection parsePredefined(ColumnSelection.Predefined predefined) {
        if (ColumnSelection.Predefined.supportedSelections.contains(predefined)) {
            return predefinedSelectionMap.get(predefined);
        } else {
            throw new UnsupportedOperationException("Selection " + predefined + " is not supported.");
        }
    }

    @Override
    public List<String> getMatchedColumns(ColumnSelection selection) {
        List<String> columnNames = new ArrayList<>();
        for (ColumnSelection.Column column : selection.getColumns()) {
            ExternalColumn externalColumn = externalColumnService.getMetadataColumn(column.getExternalColumnId());
            if (externalColumn != null) {
                columnNames.add(externalColumn.getDefaultColumnName());
            } else {
                columnNames.add(column.getColumnName());
            }
        }
        return columnNames;
    }

    @Override
    public Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection) {
        Map<String, Set<String>> partitionColumnMap = new HashMap<>();
        for (ColumnSelection.Column column : selection.getColumns()) {
            String colId = column.getExternalColumnId();
            ExternalColumn col = externalColumnService.getMetadataColumn(colId);
            String partition = col.getTablePartition();
            if (partitionColumnMap.containsKey(partition)) {
                partitionColumnMap.get(partition).add(col.getDefaultColumnName());
            } else if (StringUtils.isNotEmpty(col.getTablePartition())) {
                Set<String> set = new HashSet<>();
                set.add(col.getDefaultColumnName());
                partitionColumnMap.put(partition, set);
            }
        }
        return partitionColumnMap;
    }

    @Override
    public String getCurrentVersion(ColumnSelection.Predefined predefined) {
        return "1.0";
    }

    @Override
    public Boolean isValidVersion(ColumnSelection.Predefined predefined, String version) {
        return "1.0".equals(version);
    }

    private void loadCaches() {
        for (ColumnSelection.Predefined selection : ColumnSelection.Predefined.supportedSelections) {
            try {
                List<ExternalColumn> externalColumns = externalColumnService.findByColumnSelection(selection);
                predefinedSelectionMap.put(selection, new ColumnSelection(externalColumns));
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

}
