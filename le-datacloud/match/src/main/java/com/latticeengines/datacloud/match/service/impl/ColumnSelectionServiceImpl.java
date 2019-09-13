package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMRelease;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

@Component("columnSelectionService")
public class ColumnSelectionServiceImpl implements ColumnSelectionService {

    @Resource(name = "externalColumnService")
    private MetadataColumnService<ExternalColumn> externalColumnService;

    /*
     * Loaded from base cache of metadata in BaseMetadataColumnServiceImpl. Have
     * refresh dependency on watcher node AMReleaseBaseCache
     */
    // predefined column selection name -> column selection (consisted of a list
    // of columns)
    private WatcherCache<Predefined, ColumnSelection> predefinedSelectionCache;

    @Value("${datacloud.match.latest.rts.cache.version:1.0.0}")
    private String latestRtsCache;

    @PostConstruct
    private void postConstruct() {
        initCache();
    }

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForRTSBasedMatch(version);
    }

    @Override
    public ColumnSelection parsePredefinedColumnSelection(Predefined predefined, String dataCloudVersion) {
        if (Predefined.supportedSelections.contains(predefined)) {
            return predefinedSelectionCache.get(predefined);
        } else {
            throw new UnsupportedOperationException("Selection " + predefined + " is not supported.");
        }
    }

    @Override
    public Map<String, Set<String>> getPartitionColumnMap(ColumnSelection selection) {
        Map<String, Set<String>> partitionColumnMap = new HashMap<>();
        for (Column column : selection.getColumns()) {
            String colId = column.getExternalColumnId();
            ExternalColumn col = externalColumnService.getMetadataColumn(colId, latestRtsCache);
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

    @SuppressWarnings("unchecked")
    private void initCache() {
        predefinedSelectionCache = WatcherCache.builder() //
                .name("PredefinedSelectionCacheForRTSCache") //
                .watch(AMRelease.name()) //
                .maximum(10) //
                .load(selection -> {
                    List<ExternalColumn> externalColumns = externalColumnService
                            .findByColumnSelection((Predefined) selection, getCurrentVersion((Predefined) selection));
                    ColumnSelection cs = new ColumnSelection();
                    cs.createColumnSelection(externalColumns);
                    return cs;
                }) //
                .initKeys(Predefined.supportedSelections.toArray(new Predefined[Predefined.supportedSelections.size()])) //
                // .waitBeforeRefreshInSec((int) (Math.random() * 30))
                .build();
    }

}
