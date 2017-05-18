package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;
import java.util.Date;
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.newrelic.api.agent.Trace;

@Component("columnSelectionService")
public class ColumnSelectionServiceImpl implements ColumnSelectionService {

    private Log log = LogFactory.getLog(ColumnSelectionServiceImpl.class);

    @Value("${datacloud.match.columnselection.rts.refresh.minute:11}")
    private long refreshInterval;

    @Resource(name = "externalColumnService")
    private MetadataColumnService<ExternalColumn> externalColumnService;

    private ConcurrentMap<Predefined, ColumnSelection> predefinedSelectionMap = new ConcurrentHashMap<>();

    @Value("${datacloud.match.latest.rts.cache.version:1.0.0}")
    private String latstRtsCache;

    @Autowired
    @Qualifier("commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @PostConstruct
    private void postConstruct() {
        loadCaches();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCaches();
            }
        }, new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(refreshInterval)),
                TimeUnit.MINUTES.toMillis(refreshInterval));
    }

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForRTSBasedMatch(version);
    }

    @Override
    public ColumnSelection parsePredefinedColumnSelection(Predefined predefined, String dataCloudVersion) {
        if (Predefined.supportedSelections.contains(predefined)) {
            return predefinedSelectionMap.get(predefined);
        } else {
            throw new UnsupportedOperationException("Selection " + predefined + " is not supported.");
        }
    }

    @Override
    public List<String> getMatchedColumns(ColumnSelection selection) {
        return selection.getColumnIds();
    }

    @Override
    public Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection) {
        Map<String, Set<String>> partitionColumnMap = new HashMap<>();
        for (Column column : selection.getColumns()) {
            String colId = column.getExternalColumnId();
            ExternalColumn col = externalColumnService.getMetadataColumn(colId, latstRtsCache);
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
    public String getCurrentVersion(Predefined predefined) {
        return "1.0";
    }

    @Override
    public Map<String, Pair<BitCodeBook, List<String>>> getDecodeParameters(ColumnSelection columnSelection,
            String dataCloudVersion) {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, List<String>> getEncodedColumnMapping(ColumnSelection columnSelection, String dataCloudVersion) {
        return Collections.emptyMap();
    }

    @Trace(dispatcher = true)
    private void loadCaches() {
        for (Predefined selection : Predefined.supportedSelections) {
            try {
                List<ExternalColumn> externalColumns = externalColumnService.findByColumnSelection(selection,
                        getCurrentVersion(null));
                ColumnSelection cs = new ColumnSelection();
                cs.createColumnSelection(externalColumns);
                predefinedSelectionMap.put(selection, cs);
            } catch (Exception e) {
                log.error("Failed to load Cache!", e);
            }
        }
    }

}
