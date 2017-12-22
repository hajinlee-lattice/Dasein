package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataLakeService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class DataLakeServiceImpl implements DataLakeService {

    private static final Logger log = LoggerFactory.getLogger(DataLakeServiceImpl.class);

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private AttributeCustomizationService attributeCustomizationService;

    @Autowired
    private CacheManager cacheManager;

    private final DataLakeService _dataLakeService;

    @Autowired
    private EntityProxy entityProxy;

    private LocalCacheManager<String, Statistics> statsCache;
    private LocalCacheManager<String, List<ColumnMetadata>> cmCache;

    @Autowired
    public DataLakeServiceImpl(DataLakeService dataLakeService) {
        _dataLakeService = dataLakeService;
        statsCache = new LocalCacheManager<>(CacheName.DataLakeStatsCache, o -> {
            String str = (String) o;
            String[] tokens = str.split("\\|");
            String customerSpace = tokens[0];
            return getStatistics(customerSpace);
        }, 100); //

        cmCache = new LocalCacheManager<>(CacheName.DataLakeCMCache, o -> {
            String str = (String) o;
            String[] tokens = str.split("\\|");
            TableRoleInCollection role = TableRoleInCollection.valueOf(tokens[1]);
            String customerSpace = tokens[0];
            return getAttributesInTableRole(customerSpace, role);
        }, 100); //
    }

    @PostConstruct
    private void postConstruct() {
        if (cacheManager instanceof CompositeCacheManager) {
            log.info("adding local entity cache manager to composite cache manager");
            ((CompositeCacheManager) cacheManager).setCacheManagers(Arrays.asList(statsCache, cmCache));
        }
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
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        List<ColumnMetadata> cms = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.SEGMENT_ENTITIES) {
            cms.addAll(getAttributesInEntity(customerSpace, entity));
        }
        return cms;
    }

    @Override
    public List<ColumnMetadata> getAttributesInPredefinedGroup(ColumnSelection.Predefined predefined) {
        // Only return attributes for account now
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        List<ColumnMetadata> cms = getAttributesInEntity(customerSpace, BusinessEntity.Account);
        return cms.stream().filter(cm -> cm.getGroups() != null && cm.getGroups().contains(predefined)) //
                .collect(Collectors.toList());
    }

    @Override
    public StatsCube getStatsCube() {
        Statistics statistics = _dataLakeService.getStatistics();
        if (statistics == null) {
            return null;
        }
        return StatsCubeUtils.toStatsCube(statistics);
    }

    @Override
    public Map<BusinessEntity, StatsCube> getStatsCubes() {
        Statistics statistics = _dataLakeService.getStatistics();
        if (statistics == null) {
            return null;
        }
        return StatsCubeUtils.toStatsCubes(statistics);
    }

    @Override
    public TopNTree getTopNTree(boolean includeTopBkt) {
        Statistics statistics = _dataLakeService.getStatistics();
        if (statistics == null) {
            return null;
        }
        return StatsCubeUtils.toTopNTree(statistics, includeTopBkt);
    }

    @Override
    public AttributeStats getAttributeStats(BusinessEntity entity, String attribute) {
        Statistics statistics = _dataLakeService.getStatistics();
        if (statistics == null) {
            return null;
        }
        AttributeLookup lookup = new AttributeLookup(entity, attribute);
        for (CategoryStatistics catStats : statistics.getCategories().values()) {
            for (SubcategoryStatistics subCatStats : catStats.getSubcategories().values()) {
                if (subCatStats.getAttributes() != null && subCatStats.getAttributes().containsKey(lookup)) {
                    return subCatStats.getAttrStats(lookup);
                }
            }
        }
        log.warn("Did not find attribute stats for " + lookup);
        return null;
    }

    @Override
    public DataPage getAccountById(String accountID, ColumnSelection.Predefined predefined) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();

        List<Restriction> restrictions = new ArrayList<>();

        List<String> attributes = getAttributesInPredefinedGroup(predefined).stream() //
                .map(ColumnMetadata::getColumnId).collect(Collectors.toList());

        attributes.add(InterfaceName.AccountId.name());
        attributes.add(InterfaceName.SalesforceAccountID.name());

        if (!StringUtils.isNotEmpty(accountID)) {
            throw new LedpException(LedpCode.LEDP_39001, new String[] { accountID, customerSpace });
        }

        Restriction accRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.AccountId.name()) //
                .eq(accountID) //
                .build();

        Restriction sfdcRestriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.SalesforceAccountID.name()) //
                .eq(accountID) //
                .build();
        Restriction restriction = Restriction.builder().or(Arrays.asList(accRestriction, sfdcRestriction)).build();

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(new FrontEndRestriction(restriction));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.setRestrictNotNullSalesforceId(true);
        frontEndQuery.addLookups(BusinessEntity.Account, attributes.toArray(new String[attributes.size()]));

        log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
        return entityProxy.getData(customerSpace, frontEndQuery);
    }

    private List<ColumnMetadata> getAttributesInEntity(String customerSpace, BusinessEntity entity) {
        TableRoleInCollection role = entity.getServingStore();
        if (role == null) {
            return Collections.emptyList();
        }
        List<ColumnMetadata> cms = _dataLakeService.getAttributesInTableRole(customerSpace, role);
        cms.forEach(cm -> cm.setEntity(entity));
        return cms;
    }

    private void personalize(List<ColumnMetadata> list) {
        attributeCustomizationService
                .addFlags(list.stream().map(c -> (HasAttributeCustomizations) c).collect(Collectors.toList()));
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeStatsCacheName, key = "T(java.lang.String).format(\"%s|stats\", "
            + "T(com.latticeengines.security.exposed.util.MultiTenantContext).tenant.id)")
    public Statistics getStatistics() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        return getStatistics(customerSpace);
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

    @Cacheable(cacheNames = CacheName.Constants.DataLakeCMCacheName, key = "T(java.lang.String).format(\"%s|%s|columnmetadata\", "
            + "T(com.latticeengines.security.exposed.util.MultiTenantContext).tenant.id, #role)")
    public List<ColumnMetadata> getAttributesInTableRole(String customerSpace, TableRoleInCollection role) {
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
        Statistics statistics = getStatistics(customerSpace);
        Set<String> includedAttrs = new HashSet<>();
        statistics.getCategories().forEach((cat, catStats) -> //
        catStats.getSubcategories().forEach((subCat, subCatStats) -> //
        subCatStats.getAttributes().keySet().forEach(attrLookup -> //
        includedAttrs.add(attrLookup.getAttribute()))));
        return includedAttrs;
    }

}
