package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.exposed.util.ImportanceOrderingUtils;
import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

import reactor.core.publisher.Flux;

@Component("dataLakeService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class DataLakeServiceImpl implements DataLakeService {

    private static final Logger log = LoggerFactory.getLogger(DataLakeServiceImpl.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private CacheManager cacheManager;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private MatchProxy matchProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private MetadataProxy metadataProxy;

    private final DataLakeServiceImpl _dataLakeService;

    private final List<String> LOOKUP_FIELDS;
    private final Map<MatchKey, List<String>> KEY_MAP;

    private LocalCacheManager<String, Map<String, StatsCube>> statsCubesCache;
    private LocalCacheManager<String, List<ColumnMetadata>> cmCache;
    private ExecutorService workers = null;

    @Inject
    public DataLakeServiceImpl(DataLakeServiceImpl dataLakeService) {
        _dataLakeService = dataLakeService;
        statsCubesCache = new LocalCacheManager<>(CacheName.DataLakeStatsCubesCache, str -> {
            String[] tokens = str.split("\\|");
            String customerSpace = tokens[0];
            return getStatsCubes(customerSpace);
        }, 100); //
        cmCache = new LocalCacheManager<>(CacheName.DataLakeCMCache, str -> {
            String[] tokens = str.split("\\|");
            BusinessEntity entity = BusinessEntity.valueOf(tokens[1]);
            String customerSpace = tokens[0];
            return getServingMetadataForEntity(customerSpace, entity).collectList().block();
        }, 100); //

        LOOKUP_FIELDS = Collections.singletonList(InterfaceName.AccountId.name());
        KEY_MAP = new HashMap<>();
        KEY_MAP.put(MatchKey.LookupId, LOOKUP_FIELDS);
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
        return getAllSegmentAttributes().count().blockOptional().orElse(0L);
    }

    @Override
    public List<ColumnMetadata> getAttributes(Integer offset, Integer max) {
        Flux<ColumnMetadata> stream = getAllSegmentAttributes();
        try {
            if (offset != null) {
                stream = stream.skip(offset);
            }
            if (max != null) {
                stream = stream.take(max);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18143);
        }
        return stream.collectList().block();
    }

    private Flux<ColumnMetadata> getAllSegmentAttributes() {
        String tenantId = MultiTenantContext.getTenantId();
        Map<BusinessEntity, List<ColumnMetadata>> metadataInMds = new HashMap<>();
        Map<BusinessEntity, Set<String>> colsInServingStore = new HashMap<>();
        Map<String, StatsCube> cubeMap = new HashMap<>();
        collectMetadataInParallel(tenantId, BusinessEntity.SEGMENT_ENTITIES, metadataInMds, colsInServingStore,
                cubeMap);
        return Flux.fromIterable(BusinessEntity.SEGMENT_ENTITIES) //
                .flatMap(entity -> {
                    List<ColumnMetadata> cms = metadataInMds.getOrDefault(entity, Collections.emptyList());
                    StatsCube statsCube = cubeMap.get(entity.name());
                    if (CollectionUtils.isEmpty(cms) || statsCube == null) {
                        return Flux.empty();
                    } else {
                        Flux<ColumnMetadata> flux = Flux.fromIterable(cms);
                        Set<String> availableAttrs = colsInServingStore.getOrDefault(entity, Collections.emptySet());
                        flux = flux //
                                .filter(cm -> cm.isEnabledFor(ColumnSelection.Predefined.Segment)) //
                                .filter(cm -> !StatsCubeUtils.shouldHideAttr(entity, cm)) //
                                .filter(cm -> {
                                    boolean avail = availableAttrs.contains(cm.getAttrName());
                                    if (!avail) {
                                        log.info("Filtering out  " + cm.getAttrName() + " in " + entity
                                                + " because it is not in serving store.");
                                    }
                                    return avail;
                                });
                        return StatsCubeUtils.sortByCategory(flux, statsCube);
                    }
                });
    }

    @Override
    public List<ColumnMetadata> getAttributesInPredefinedGroup(ColumnSelection.Predefined predefined) {
        // Only return attributes for account now
        String tenantId = MultiTenantContext.getTenantId();
        List<ColumnMetadata> cms = _dataLakeService.getCachedServingMetadataForEntity(tenantId, BusinessEntity.Account);
        return cms.stream().filter(cm -> cm.getGroups() != null && cm.isEnabledFor(predefined)
        // Hack to limit attributes for talking points temporarily PLS-7065
                && (cm.getCategory().equals(Category.ACCOUNT_ATTRIBUTES)
                        || cm.getCategory().equals(Category.FIRMOGRAPHICS))) //
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, StatsCube> getStatsCubes() {
        String tenantId = MultiTenantContext.getTenantId();
        return _dataLakeService.getStatsCubes(tenantId);
    }

    @Override
    public TopNTree getTopNTree() {
        String tenantId = MultiTenantContext.getTenantId();
        return _dataLakeService.getTopNTree(tenantId);
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
    public DataPage getAccountById(String accountId, ColumnSelection.Predefined predefined,
            Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();

        if (!StringUtils.isNotEmpty(accountId)) {
            throw new LedpException(LedpCode.LEDP_39001, new String[] { accountId, customerSpace });
        }

        String lookupIdColumn = lookupIdMappingProxy.findLookupIdColumn(orgInfo, customerSpace);

        String internalAccountId = getInternalAccountIdViaObjectApi(customerSpace, accountId, lookupIdColumn);

        DataPage dataPage = null;

        if (StringUtils.isNotBlank(internalAccountId)) {
            dataPage = getAccountByIdViaMatchApi(customerSpace, internalAccountId, predefined);

            if (dataPage == null || CollectionUtils.isEmpty(dataPage.getData())) {
                // if we didn't get any data from matchapi then it may be
                // because data is not published to dynamoDB for this tenant. So
                // for fallback mechanism we'll use original logic to get data
                // from redshift

                log.info("Falling back to old logic for extracting account data from Redshift");

                List<String> attributes = getAttributesInPredefinedGroup(predefined).stream() //
                        .map(ColumnMetadata::getAttrName).collect(Collectors.toList());
                try {
                    dataPage = getAccountDataViaObjectApi(customerSpace, accountId, lookupIdColumn, attributes, true);
                } catch (Exception ex) {
                    log.info("Ignoring error due to missing lookup id column. Trying without lookup id this time.", ex);
                    dataPage = getAccountDataViaObjectApi(customerSpace, accountId, lookupIdColumn, attributes, false);
                }
            }

            if (dataPage != null && dataPage.getData() != null && dataPage.getData().size() == 1) {
                if (!dataPage.getData().get(0).containsKey(InterfaceName.AccountId.name())) {
                    dataPage.getData().get(0).put(InterfaceName.AccountId.name(), internalAccountId);
                }
            }
        } else {
            dataPage = createEmptyDataPage();
        }

        return dataPage;
    }

    private DataPage getAccountByIdViaMatchApi(String customerSpace, String internalAccountId,
            ColumnSelection.Predefined predefined) {

        MatchInput matchInput = new MatchInput();

        List<List<Object>> data = new ArrayList<>();
        List<Object> datum = new ArrayList<>();

        datum.add(internalAccountId);
        data.add(datum);

        Tenant tenant = new Tenant(customerSpace);
        matchInput.setTenant(tenant);
        matchInput.setFields(LOOKUP_FIELDS);
        matchInput.setData(data);
        matchInput.setKeyMap(KEY_MAP);
        matchInput.setPredefinedSelection(predefined);
        String dataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        matchInput.setUseRemoteDnB(false);
        matchInput.setDataCloudVersion(dataCloudVersion);

        log.info(String.format("Calling matchapi with request payload: %s", JsonUtils.serialize(matchInput)));
        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        return convertToDataPage(matchOutput);
    }

    private DataPage convertToDataPage(MatchOutput matchOutput) {
        DataPage dataPage = createEmptyDataPage();
        Map<String, Object> data = null;
        if (matchOutput != null //
                && CollectionUtils.isNotEmpty(matchOutput.getResult()) //
                && matchOutput.getResult().get(0) != null) {

            if (matchOutput.getResult().get(0).isMatched() != Boolean.TRUE) {
                log.info("Didn't find any match from lattice data cloud. "
                        + "Still continue to process the result as we may "
                        + "have found partial match in my data table.");
            } else {
                log.info("Found full match from lattice data cloud as well as from my data table.");
            }

            final Map<String, Object> tempDataRef = new HashMap<>();
            List<String> fields = matchOutput.getOutputFields();
            List<Object> values = matchOutput.getResult().get(0).getOutput();
            IntStream.range(0, fields.size()) //
                    .forEach(i -> {
                        tempDataRef.put(fields.get(i), values.get(i));
                    });
            data = tempDataRef;

        }

        if (MapUtils.isNotEmpty(data)) {
            dataPage.getData().add(data);
        }
        return dataPage;
    }

    private DataPage createEmptyDataPage() {
        DataPage dataPage = new DataPage();
        List<Map<String, Object>> dataList = new ArrayList<>();
        dataPage.setData(dataList);
        return dataPage;
    }

    private String getInternalAccountIdViaObjectApi(String customerSpace, String accountId, String lookupIdColumn) {
        List<String> attributes = Collections.singletonList(InterfaceName.AccountId.name());
        DataPage dataPage = null;
        try {
            dataPage = getAccountDataViaObjectApi(customerSpace, accountId, lookupIdColumn, attributes, true);
        } catch (Exception ex) {
            log.info("Ignoring error due to missing lookup id column. Trying without lookup id this time.", ex);
            dataPage = getAccountDataViaObjectApi(customerSpace, accountId, lookupIdColumn, attributes, false);
        }
        String internalAccountId = null;

        if (dataPage != null && CollectionUtils.isNotEmpty(dataPage.getData())) {
            Object internalAccountIdObj = dataPage.getData().get(0).get(InterfaceName.AccountId.name());
            if (internalAccountIdObj != null) {
                internalAccountId = internalAccountIdObj.toString();
            }
        }

        return internalAccountId;
    }

    private DataPage getAccountDataViaObjectApi(String customerSpace, String accountId, String lookupIdColumn,
            List<String> attributes, boolean shouldAddLookupIdClause) {

        Restriction restriction = Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.AccountId.name()) //
                .eq(accountId) //
                .build();

        if (shouldAddLookupIdClause) {
            Restriction sfdcRestriction = Restriction.builder() //
                    .let(BusinessEntity.Account, lookupIdColumn) //
                    .eq(accountId) //
                    .build();

            restriction = Restriction.builder().or(Arrays.asList(restriction, sfdcRestriction)).build();
        }

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(new FrontEndRestriction(restriction));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.addLookups(BusinessEntity.Account, attributes.toArray(new String[attributes.size()]));

        log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
        return entityProxy.getData(customerSpace, frontEndQuery);
    }

    @Override
    public synchronized TopNTree getTopNTree(String customerSpace) {
        return _dataLakeService.getTopNTreeFromCache(customerSpace);
    }

    @Override
    public synchronized Map<String, StatsCube> getStatsCubes(String customerSpace) {
        return _dataLakeService.getStatsCubesFromCache(customerSpace);
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeStatsCubesCache, key = "T(java.lang.String).format(\"%s|topn\", #customerSpace)", unless = "#result == null")
    public TopNTree getTopNTreeFromCache(String customerSpace) {
        Map<String, StatsCube> cubes = new HashMap<>();
        Map<String, List<ColumnMetadata>> cmMap = new HashMap<>();
        populateColumnMetadataMap(customerSpace, cmMap, cubes);
        if (MapUtils.isEmpty(cubes) || MapUtils.isEmpty(cmMap)) {
            return null;
        }
        String timerMsg = "Construct top N tree with " + cubes.size() + " cubes.";
        try (PerformanceTimer timer = new PerformanceTimer(timerMsg)) {
            return StatsCubeUtils.constructTopNTree(cubes, cmMap, true, ColumnSelection.Predefined.Segment);
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeStatsCubesCache, key = "T(java.lang.String).format(\"%s|statscubes\", #customerSpace)", unless = "#result == null")
    public Map<String, StatsCube> getStatsCubesFromCache(String customerSpace) {
        StatisticsContainer container = dataCollectionProxy.getStats(customerSpace);
        if (container != null) {
            return container.getStatsCubes();
        }
        return null;
    }

    private void populateColumnMetadataMap(String customerSpace, Map<String, List<ColumnMetadata>> cmMap,
            Map<String, StatsCube> cubeMap) {
        Map<BusinessEntity, List<ColumnMetadata>> metadataMap = new HashMap<>();
        Map<BusinessEntity, Set<String>> columnNamesMap = new HashMap<>();
        collectMetadataInParallel(customerSpace, BusinessEntity.SEGMENT_ENTITIES, metadataMap, columnNamesMap, cubeMap);
        ConcurrentMap<String, List<ColumnMetadata>> concurrentCmMap = new ConcurrentHashMap<>();
        cubeMap.keySet().forEach(key -> {
            BusinessEntity entity = BusinessEntity.valueOf(key);
            List<ColumnMetadata> cms = metadataMap.get(entity);
            Set<String> attrs = columnNamesMap.getOrDefault(entity, Collections.emptySet());
            if (CollectionUtils.isNotEmpty(cms)) {
                List<ColumnMetadata> attrsInTable = Flux.fromIterable(cms)
                        .filter(cm -> attrs.contains(cm.getAttrName())).collectList().block();
                concurrentCmMap.put(key, attrsInTable);
            }
        });
        cmMap.clear();
        cmMap.putAll(concurrentCmMap);
    }

    private void collectMetadataInParallel(String customerSpace, Collection<BusinessEntity> entities,
            Map<BusinessEntity, List<ColumnMetadata>> metadataMap, Map<BusinessEntity, Set<String>> columnNamesMap,
            Map<String, StatsCube> statsCubeMap) {
        List<Runnable> runnables = new ArrayList<>();
        ConcurrentMap<BusinessEntity, List<ColumnMetadata>> concurrentMetadataMap = new ConcurrentHashMap<>();
        ConcurrentMap<BusinessEntity, Set<String>> concurrentColumnNamesMap = new ConcurrentHashMap<>();
        ConcurrentMap<String, StatsCube> concurrentStatsCubeMap = new ConcurrentHashMap<>();
        final String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace);
        StopWatch stopWatch = new StopWatch();
        entities.forEach(entity -> {
            Runnable metadataRunnable = //
                    () -> {
                        log.info("Getting cached metadata for " + tenantId + " : " + entity);
                        List<ColumnMetadata> cms = _dataLakeService.getCachedServingMetadataForEntity(tenantId, entity);
                        if (CollectionUtils.isNotEmpty(cms)) {
                            concurrentMetadataMap.put(entity, cms);
                        }
                        log.info("Finished getting cached metadata for " + tenantId + " : " + entity);
                    };
            Runnable columnNamesRunnable = //
                    () -> {
                        log.info("Getting serving store attrs for " + tenantId + " : " + entity);
                        Set<String> attrs = _dataLakeService.getServingTableColumns(customerSpace, entity);
                        if (CollectionUtils.isNotEmpty(attrs)) {
                            concurrentColumnNamesMap.put(entity, attrs);
                        }
                        log.info("Finished getting serving store attrs for " + tenantId + " : " + entity);
                    };
            runnables.add(metadataRunnable);
            runnables.add(columnNamesRunnable);
        });
        Runnable statsCubeRunnable = () -> {
            log.info("Getting stats cubes for " + tenantId);
            Map<String, StatsCube> cubes = _dataLakeService.getStatsCubesFromCache(tenantId);
            if (MapUtils.isNotEmpty(cubes)) {
                concurrentStatsCubeMap.putAll(cubes);
            }
            log.info("Finished getting stats cubes for " + tenantId);
        };
        runnables.add(statsCubeRunnable);
        ThreadPoolUtils.runRunnablesInParallel(getWorkers(), runnables, 30, 2);
        metadataMap.putAll(concurrentMetadataMap);
        columnNamesMap.putAll(concurrentColumnNamesMap);
        statsCubeMap.putAll(concurrentStatsCubeMap);
        log.info("Finished collection metadata artifacts in parallel. Used " + stopWatch.getTime(TimeUnit.SECONDS)
                + " sec.");
    }

    public List<ColumnMetadata> getCachedServingMetadataForEntity(String customerSpace, BusinessEntity entity) {
        return getServingMetadataForEntity(customerSpace, entity).collectList().block();
    }

    private Flux<ColumnMetadata> getServingMetadataForEntity(String customerSpace, BusinessEntity entity) {
        Flux<ColumnMetadata> cms = Flux.empty();
        if (entity != null) {
            List<ColumnMetadata> cmList = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, entity);
            if (CollectionUtils.isNotEmpty(cmList)) {
                cms = Flux.fromIterable(cmList) //
                        .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState()));
                if (BusinessEntity.Account.equals(entity)) {
                    cms = ImportanceOrderingUtils.addImportanceOrdering(cms);
                }
                AtomicLong counter = new AtomicLong();
                AtomicLong timer = new AtomicLong();
                cms = cms.doOnSubscribe(s -> timer.set(System.currentTimeMillis())) //
                        .doOnNext(cm -> counter.getAndIncrement()).doOnComplete(() -> {
                            long duration = System.currentTimeMillis() - timer.get();
                            long count = counter.get();
                            String msg = "Retrieved " + count + " attributes from serving store proxy for " + entity
                                    + " in " + customerSpace + " TimeElapsed=" + duration + " msec.";
                            log.info(msg);
                        });
            }
        }
        return cms;
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeCMCacheName, key = "T(java.lang.String).format(\"%s|%s|servingtable\", #customerSpace, #entity)", unless = "#result == null")
    public Set<String> getServingTableColumns(String customerSpace, BusinessEntity entity) {
        Set<String> result = null;
        if (entity != null) {
            String tableName = dataCollectionProxy.getTableName(customerSpace, entity.getServingStore());
            if (StringUtils.isBlank(tableName)) {
                log.info("Cannot find serving table for " + entity + " in " + customerSpace);
            } else {
                try (PerformanceTimer timer = new PerformanceTimer()) {
                    Set<String> attrSet = new HashSet<>();
                    List<ColumnMetadata> cms = metadataProxy.getTableColumns(customerSpace, tableName);
                    cms.forEach(cm -> attrSet.add(cm.getAttrName()));
                    timer.setTimerMessage(
                            "Fetched " + attrSet.size() + " attr names for " + entity + " in " + customerSpace);
                    if (CollectionUtils.isNotEmpty(attrSet)) {
                        result = attrSet;
                    }
                }
            }
        }
        return result;
    }

    @VisibleForTesting
    void setMatchProxy(MatchProxy matchProxy) {
        this.matchProxy = matchProxy;
    }

    private ExecutorService getWorkers() {
        if (workers == null) {
            synchronized (this) {
                if (workers == null) {
                    workers = ThreadPoolUtils.getFixedSizeThreadPool("datalake-svc", 8);
                }
            }
        }
        return workers;
    }
}
