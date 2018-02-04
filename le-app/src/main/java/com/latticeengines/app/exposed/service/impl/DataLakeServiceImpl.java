package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.exposed.util.ImportanceOrderingUtils;
import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.metadata.transaction.TransactionMetrics;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

@Component("dataLakeService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class DataLakeServiceImpl implements DataLakeService {

    private static final Logger log = LoggerFactory.getLogger(DataLakeServiceImpl.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private AttributeCustomizationService attributeCustomizationService;

    @Inject
    private CacheManager cacheManager;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    private final DataLakeService _dataLakeService;

    private LocalCacheManager<String, Map<String, StatsCube>> statsCubesCache;
    private LocalCacheManager<String, List<ColumnMetadata>> cmCache;

    @Inject
    public DataLakeServiceImpl(DataLakeService dataLakeService) {
        _dataLakeService = dataLakeService;
        statsCubesCache = new LocalCacheManager<>(CacheName.DataLakeStatsCubesCache, str -> {
            String[] tokens = str.split("\\|");
            String customerSpace = tokens[0];
            return getStatsCubes(customerSpace);
        }, 100); //

        cmCache = new LocalCacheManager<>(CacheName.DataLakeCMCache, str -> {
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
            ((CompositeCacheManager) cacheManager).setCacheManagers(Arrays.asList(statsCubesCache, cmCache));
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
        return cms.parallelStream() //
                .filter(cm -> !StatsCubeUtils.shouldHideAttr(cm)).collect(Collectors.toList());
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
    public Map<String, StatsCube> getStatsCubes() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        return _dataLakeService.getStatsCubes(customerSpace);
    }

    @Override
    public TopNTree getTopNTree() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        TopNTree topNTree = _dataLakeService.getTopNTree(customerSpace);
        if (topNTree.hasCategory(Category.RATING)) {
            List<RatingEngineSummary> engineSummaries = getRatingSummaries(customerSpace);
            StatsCubeUtils.processRatingCategory(topNTree, engineSummaries);
        }
        return topNTree;
    }

    @Override
    public AttributeStats getAttributeStats(BusinessEntity entity, String attribute) {
        Map<String, StatsCube> cubes = getStatsCubes();
        if (MapUtils.isEmpty(cubes) || !cubes.containsKey(entity.name())) {
            return null;
        }
        StatsCube cube = cubes.get(entity.name());
        if (cube.getStatistics().containsKey(attribute)) {
            return cube.getStatistics().get(attribute);
        } else {
            log.warn("Did not find attribute stats for " + entity + "." + attribute);
            return null;
        }
    }

    @Override
    public DataPage getAccountById(String accountID, ColumnSelection.Predefined predefined) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();

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
        if (BusinessEntity.Rating.equals(entity)) {
            List<RatingEngineSummary> engineSummaries = getRatingSummaries(customerSpace);
            StatsCubeUtils.injectRatingEngineMetadata(cms, engineSummaries);
        }
        return cms;
    }

    private void personalize(List<ColumnMetadata> list) {
        attributeCustomizationService
                .addFlags(list.stream().map(c -> (HasAttributeCustomizations) c).collect(Collectors.toList()));
    }

    private Map<String, String> getProductMap(String customerSpace) {
        String tableName = dataCollectionProxy.getTableName(customerSpace, BusinessEntity.Product.getServingStore());
        if (StringUtils.isNotBlank(tableName)) {
            FrontEndQuery frontEndQuery = new FrontEndQuery();
            frontEndQuery.setMainEntity(BusinessEntity.Product);
            DataPage dataPage = entityProxy.getData(customerSpace, frontEndQuery);
            Map<String, String> productMap = new HashMap<>();
            dataPage.getData()
                    .forEach(row -> productMap.put( //
                            (String) row.get(InterfaceName.ProductId.name()), //
                            (String) row.get(InterfaceName.ProductName.name()) //
                    ));
            return productMap;
        } else {
            return Collections.emptyMap();
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeStatsCubesCache, key = "T(java.lang.String).format(\"%s|topn\", "
            + "T(com.latticeengines.db.exposed.util.MultiTenantContext).tenant.id)")
    public TopNTree getTopNTree(String customerSpace) {
        Map<String, StatsCube> cubes = getStatsCubes();
        if (MapUtils.isEmpty(cubes)) {
            return null;
        }
        Map<String, List<ColumnMetadata>> cmMap = new HashMap<>();
        cubes.keySet().forEach(key -> {
            BusinessEntity entity = BusinessEntity.valueOf(key);
            List<ColumnMetadata> cms = getAttributesInEntity(customerSpace, entity);
            if (CollectionUtils.isNotEmpty(cms)) {
                cmMap.put(key, cms);
            }
        });
        if (MapUtils.isEmpty(cmMap)) {
            return null;
        }
        String timerMsg = "Construct top N tree with " + cubes.size() + " cubes.";
        try (PerformanceTimer timer = new PerformanceTimer(timerMsg)) {
            TopNTree topNTree = StatsCubeUtils.constructTopNTree(cubes, cmMap, true);
            if (topNTree.hasCategory(Category.PRODUCT_SPEND)) {
                Map<String, String> productMap = getProductMap(customerSpace);
                timerMsg = "Construct top N tree with " + cubes.size() + " cubes and " + productMap.size() + " products.";
                timer.setTimerMessage(timerMsg);
                StatsCubeUtils.processPurchaseHistoryCategory(topNTree, productMap);
            }
            return topNTree;
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeStatsCubesCache, key = "T(java.lang.String).format(\"%s|statscubes\", "
            + "T(com.latticeengines.db.exposed.util.MultiTenantContext).tenant.id)")
    public Map<String, StatsCube> getStatsCubes(String customerSpace) {
        StatisticsContainer container = dataCollectionProxy.getStats(customerSpace);
        if (container != null) {
            return container.getStatsCubes();
        }
        return null;
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeCMCacheName, key = "T(java.lang.String).format(\"%s|%s|columnmetadata\", "
            + "T(com.latticeengines.db.exposed.util.MultiTenantContext).tenant.id, #role)")
    public List<ColumnMetadata> getAttributesInTableRole(String customerSpace, TableRoleInCollection role) {
        if (role == null) {
            return Collections.emptyList();
        }
        String batchTableName= dataCollectionProxy.getTableName(customerSpace, role);
        if (StringUtils.isBlank(batchTableName)) {
            return Collections.emptyList();
        } else {
            List<ColumnMetadata> cms = metadataProxy.getTableColumns(customerSpace, batchTableName);
            if (BusinessEntity.Account.getServingStore().equals(role)) {
                ImportanceOrderingUtils.addImportanceOrdering(cms);
            } else if (BusinessEntity.PurchaseHistory.getServingStore().equals(role)) {
                Map<String, String> productMap = getProductMap(customerSpace);
                cms.forEach(cm -> {
                    String prodId = TransactionMetrics.getProductIdFromAttr(cm.getName());
                    String prodName = productMap.getOrDefault(prodId, "Other");
                    cm.setSubcategory(prodName);
                });
            }
            return cms;
        }
    }


    private List<RatingEngineSummary> getRatingSummaries(String customerSpace) {
        List<RatingEngineSummary> engineSummaries = new ArrayList<>();
        try {
            engineSummaries = ratingEngineProxy.getRatingEngineSummaries(customerSpace);
        } catch (Exception e) {
            log.warn("Failed to retrieve engine summaries.", e);
        }
        return engineSummaries;
    }
}
