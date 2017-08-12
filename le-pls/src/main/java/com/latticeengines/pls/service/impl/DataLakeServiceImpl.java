package com.latticeengines.pls.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.CustomerMetadata;
import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.CustomerStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.pls.service.DataLakeService;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataLakeService")
public class DataLakeServiceImpl implements DataLakeService {

    private static final Logger log = LoggerFactory.getLogger(DataLakeServiceImpl.class);

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private AttributeCustomizationService attributeCustomizationService;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    private WatcherCache<String, Statistics> statsCache = null;
    private WatcherCache<String, List<ColumnMetadata>> cmCache = null;

    @PostConstruct
    private void postConstruct() {
        initStatsCache();
        initCmCache();
    }

    @Override
    public long getAttributesCount() {
        List<ColumnMetadata> cms = getAllAttributes();
        if (cms == null) {
            return 0;
        } else {
            return cms.size();
        }
    }

    @Override
    public List<ColumnMetadata> getAttributes(Integer offset, Integer max) {
        List<ColumnMetadata> cms = getAllAttributes();
        Stream<ColumnMetadata> stream = cms.stream().sorted(Comparator.comparing(ColumnMetadata::getColumnId));
        try {
            if (offset != null) {
                stream = stream.skip(offset);
            }
            if (max != null) {
                stream = stream.limit(max);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18143);
        }
        List<ColumnMetadata> list = stream.collect(Collectors.toList());
        personalize(list);
        return list;
    }

    private List<ColumnMetadata> getAllAttributes() {
        String customerSpace = MultiTenantContext.getTenant().getId();
        List<ColumnMetadata> cms = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.values()) {
            if (!BusinessEntity.LatticeAccount.equals(entity)) {
                cms.addAll(getAttributesInEntity(customerSpace, entity));
            }
        }
        // TODO: backward compatible with old account table
        if (cms.size() < 10_000) {
            Set<String> includedAttrs = getAttrsInStats(customerSpace);
            String currentDataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
            List<ColumnMetadata> amCms = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Segment,
                    currentDataCloudVersion);
            amCms.forEach(cm -> cm.setEntity(BusinessEntity.LatticeAccount));
            amCms.removeIf(cm -> !includedAttrs.contains(cm.getColumnId()));
            cms.addAll(amCms);
        }
        return cms;
    }

    @Override
    public StatsCube getStatsCube() {
        Statistics statistics = getStatistics();
        if (statistics == null) {
            return null;
        }
        return StatsCubeUtils.toStatsCube(statistics);
    }

    @Override
    public TopNTree getTopNTree(boolean includeTopBkt) {
        Statistics statistics = getStatistics();
        if (statistics == null) {
            return null;
        }
        return StatsCubeUtils.toTopNTree(statistics, includeTopBkt);
    }

    @Override
    public AttributeStats getAttributeStats(BusinessEntity entity, String attribute) {
        Statistics statistics = getStatistics();
        if (statistics == null) {
            return null;
        }
        AttributeLookup lookup = new AttributeLookup(entity, attribute);
        for (CategoryStatistics catStats: statistics.getCategories().values()) {
            for (SubcategoryStatistics subCatStats: catStats.getSubcategories().values()) {
                if (subCatStats.getAttributes() != null && subCatStats.getAttributes().containsKey(lookup)) {
                    return subCatStats.getAttrStats(lookup);
                }
            }
        }
        log.warn("Did not find attribute stats for " + lookup);
        return null;
    }

    private List<ColumnMetadata> getAttributesInEntity(String customerSpace, BusinessEntity entity) {
        TableRoleInCollection role = entity.getServingStore();
        if (role == null) {
            return Collections.emptyList();
        }
        List<ColumnMetadata> cms = cmCache.get(String.format("%s|%s", customerSpace, role.name()));
        cms.forEach(cm -> cm.setEntity(entity));
        switch (entity) {
            case Account:
                cms.forEach(cm -> {
                    if (cm.getCategory() == null) {
                        cm.setCategory(Category.ACCOUNT_ATTRIBUTES);
                    }
                });
                break;
            case Contact:
                cms.forEach(cm -> {
                    if (cm.getCategory() == null) {
                        cm.setCategory(Category.CONTACT_ATTRIBUTES);
                    }
                });
                break;
        }
        return cms;
    }

    private void personalize(List<ColumnMetadata> list) {
        attributeCustomizationService
                .addFlags(list.stream().map(c -> (HasAttributeCustomizations) c).collect(Collectors.toList()));
    }

    private Statistics getStatistics() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        initStatsCache();
        return statsCache.get(customerSpace);
    }

    @SuppressWarnings("unchecked")
    private void initStatsCache() {
        if (statsCache == null) {
            statsCache = WatcherCache.builder() //
                    .name("DataLakeStatsCache") //
                    .watch(CustomerStats) //
                    .maximum(100) //
                    .load(o -> {
                        String customerSpace = (String) o;
                        return getStatistics(customerSpace);
                    }).build();
            statsCache.setRefreshKeyResolver(s -> {
                try {
                    String customerSpace = CustomerSpace.parse(s).toString();
                    if (customerSpace.equals(s)) {
                        log.info("Attempt to refresh the stats cache of tenant " + customerSpace);
                        return Collections.singletonList(s);
                    } else {
                        throw new IllegalArgumentException(
                                "Parsed customer space " + customerSpace + " is different from the watched data " + s);
                    }
                } catch (Exception e) {
                    log.warn("Cannot parse watched data " + s + " into a customer space.", e);
                    return Collections.emptyList();
                }
            });
        }
    }

    private Statistics getStatistics(String customerSpace) {
        StatisticsContainer container = dataCollectionProxy.getStats(customerSpace);
        if (container != null) {
            Statistics statistics = container.getStatistics();
            return removeNoBktAttrs(statistics);
        }
        return null;
    }

    private Statistics removeNoBktAttrs(Statistics statistics) {
        for (Map.Entry<Category, CategoryStatistics> entry : statistics.getCategories().entrySet()) {
            statistics.putCategory(entry.getKey(), removeNoBktAttrs(entry.getValue()));
        }
        return statistics;
    }

    private CategoryStatistics removeNoBktAttrs(CategoryStatistics catStats) {
        for (Map.Entry<String, SubcategoryStatistics> entry : catStats.getSubcategories().entrySet()) {
            catStats.putSubcategory(entry.getKey(), removeNoBktAttrs(entry.getValue()));
        }
        return catStats;
    }

    private SubcategoryStatistics removeNoBktAttrs(SubcategoryStatistics subcatStats) {
        Map<AttributeLookup, AttributeStats> statsMap = new HashMap<>();
        subcatStats.getAttributes().entrySet().stream()
                .filter(entry -> entry.getValue().getBuckets() != null
                        && !entry.getValue().getBuckets().getBucketList().isEmpty())
                .forEach(entry -> statsMap.put(entry.getKey(), entry.getValue()));
        subcatStats.setAttributes(statsMap);
        return subcatStats;
    }

    @SuppressWarnings("unchecked")
    private void initCmCache() {
        if (cmCache == null) {
            cmCache = WatcherCache.builder() //
                    .name("DataLakeColumnMetadataCache") //
                    .watch(CustomerMetadata) //
                    .maximum(100) //
                    .load(o -> {
                        String str = (String) o;
                        String[] tokens = str.split("\\|");
                        TableRoleInCollection role = TableRoleInCollection.valueOf(tokens[1]);
                        String customerSpace = tokens[0];
                        return getAttributesInTableRole(customerSpace, role);
                    }).build();
            cmCache.setRefreshKeyResolver(s -> {
                try {
                    String[] tokens = s.split("\\|");
                    TableRoleInCollection role = TableRoleInCollection.valueOf(tokens[1]);
                    String customerSpace = CustomerSpace.parse(tokens[0]).toString();
                    if (customerSpace.equals(tokens[0])) {
                        log.info("Attempt to refresh the table metadata cache of " + role + " in tenant " + customerSpace);
                        return Collections.singletonList(s);
                    } else {
                        throw new IllegalArgumentException("Parsed customer space " + customerSpace
                                + " is different from the watched data " + tokens[0]);
                    }
                } catch (Exception e) {
                    log.warn("Cannot parse watched data " + s + " into a valid cache key.", e);
                    return Collections.emptyList();
                }
            });
        }
    }
    
    private List<ColumnMetadata> getAttributesInTableRole(String customerSpace, TableRoleInCollection role) {
        if (role == null) {
            return Collections.emptyList();
        }
        Table batchTable = dataCollectionProxy.getTable(customerSpace, role);
        if (batchTable == null) {
            return Collections.emptyList();
        } else {
            Set<String> includedAttrs = getAttrsInStats(customerSpace);
            return batchTable.getAttributes().stream() //
                    .map(Attribute::getColumnMetadata) //
                    .filter(cm -> includedAttrs.contains(cm.getColumnId())) //
                    .collect(Collectors.toList());
        }
    }

    private Set<String> getAttrsInStats(String customerSpace) {
        Statistics statistics = statsCache.get(customerSpace);
        Set<String> includedAttrs = new HashSet<>();
        statistics.getCategories().forEach((cat, catStats) -> //
                catStats.getSubcategories().forEach((subCat, subCatStats) -> //
                        subCatStats.getAttributes().keySet().forEach(attrLookup -> //
                                includedAttrs.add(attrLookup.getAttribute()))));
        return includedAttrs;
    }

}
