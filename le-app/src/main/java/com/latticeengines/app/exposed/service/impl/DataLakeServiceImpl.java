package com.latticeengines.app.exposed.service.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.support.CompositeCacheManager;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.exposed.util.ImportanceOrderingUtils;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cache.exposed.cachemanager.LocalCacheManager;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.AccountExtensionUtil;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
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
    private DynamoItemService dynamoItemService;

    @Inject
    private BatonService batonService;

    private final DataLakeServiceImpl _dataLakeService;

    @Value("${eai.export.dynamo.atlas.lookup.table}")
    private String dynamoAccountLookupCacheTableName;

    private final String DYNAMO_ACCOUNT_LOOKUP_CACHE_KEY_FORMAT = "{0}_{1}_{2}";
    private final String DYNAMO_ACCOUNT_LOOKUP_CACHE_KEY_FIELD = "Key";
    private final String DYNAMO_ACCOUNT_LOOKUP_CACHE_VALUE_FIELD = "AccountId";
    private final String DYNAMO_ACCOUNT_LOOKUP_CACHE_ACCOUNTID_FIELD = InterfaceName.AccountId.name();
    private final List<String> LOOKUP_FIELDS;
    private final Map<MatchKey, List<String>> KEY_MAP;

    private LocalCacheManager<String, Map<String, StatsCube>> statsCubesCache;
    private LocalCacheManager<String, List<ColumnMetadata>> cmCache;
    private LoadingCache<String, Boolean> hasAccountLookupCache = Caffeine.newBuilder() //
            .maximumSize(1000) //
            .expireAfterWrite(30, TimeUnit.SECONDS) //
            .build(this::hasAccountLookupCache);
    private LoadingCache<String, Boolean> contactsByAccountLookupsPopulatedCache = Caffeine.newBuilder() //
            .maximumSize(1000) //
            .expireAfterWrite(30, TimeUnit.SECONDS) //
            .build(this::contactsByAccountLookupsPopulated);
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

    @Override
    public Map<String, StatsCube> getStatsCubes() {
        String tenantId = MultiTenantContext.getShortTenantId();
        return _dataLakeService.getStatsCubes(tenantId);
    }

    @Override
    public TopNTree getTopNTree() {
        String tenantId = MultiTenantContext.getShortTenantId();
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
    public DataPage getContactsByAccountById(String accountIdValue, Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();

        if (!StringUtils.isNotEmpty(accountIdValue)) {
            throw new LedpException(LedpCode.LEDP_39001, new String[] { accountIdValue, customerSpace });
        }

        if (Boolean.TRUE.equals(contactsByAccountLookupsPopulatedCache.get(customerSpace))) {
            String lookupIdColumn = lookupIdMappingProxy.findLookupIdColumn(orgInfo, customerSpace);
            return new DataPage(
                    matchProxy.lookupContactsByAccountId(customerSpace, lookupIdColumn, accountIdValue, null));
        } else {
            log.warn(String.format("Contact Data not published in Dynamo for customerSpace: %s", customerSpace));
            return new DataPage(new ArrayList<>());
        }
    }

    @Override
    public DataPage getAccountById(String accountId, ColumnSelection.Predefined predefined,
            Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        String internalAccountId = getInternalAccountId(accountId, orgInfo);

        DataPage dataPage;
        if (StringUtils.isNotBlank(internalAccountId)) {
            dataPage = getAccountByIdViaMatchApi(customerSpace, internalAccountId, predefined);
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

    @Override
    public DataPage getAccountById(String accountId, List<String> lookupAttributes, Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        String internalAccountId = getInternalAccountId(accountId, orgInfo);

        DataPage dataPage;
        if (StringUtils.isNotBlank(internalAccountId)) {
            dataPage = getAccountByIdViaMatchApi(customerSpace, internalAccountId,
                    lookupAttributes.stream().map(Column::new).collect(Collectors.toList()));

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

    @Override
    public DataPage getAccountById(String accountID, ColumnSelection.Predefined predefined, Map<String, String> orgInf,
            List<String> requiredAttributes) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();

        DataPage page = getAccountById(accountID, predefined, orgInf);
        if (page == null || CollectionUtils.isEmpty(page.getData())) {
            return page;
        }
        Map<String, Object> accountData = page.getData().get(0);

        List<String> missingReqdAttributes = requiredAttributes.stream().filter(col -> !accountData.containsKey(col))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(missingReqdAttributes)) {
            return page;
        }

        Map<String, ColumnMetadata> attributesMap = getServingMetadataForEntity(customerSpace, BusinessEntity.Account)
                .collect(HashMap<String, ColumnMetadata>::new, (returnMap, cm) -> returnMap.put(cm.getAttrName(), cm))
                .block();

        missingReqdAttributes.stream().filter(col -> !attributesMap.containsKey(col)).collect(Collectors.toList())
                .forEach(col -> accountData.put(col, null));

        List<Column> missingAttributesNotInPredefinedSelections = missingReqdAttributes.stream()
                .filter(attributesMap::containsKey).map(col -> new Column(col, attributesMap.get(col).getDisplayName()))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(missingAttributesNotInPredefinedSelections)) {
            DataPage reqdAttributesPage = getAccountByIdViaMatchApi(customerSpace, accountID,
                    missingAttributesNotInPredefinedSelections);

            if (reqdAttributesPage != null && CollectionUtils.isNotEmpty(reqdAttributesPage.getData())
                    && MapUtils.isNotEmpty(reqdAttributesPage.getData().get(0))) {
                accountData.putAll(reqdAttributesPage.getData().get(0));
            } else {
                missingReqdAttributes.stream().filter(attributesMap::containsKey).collect(Collectors.toList())
                        .forEach(col -> accountData.put(col, null));
            }
        }

        return page;
    }

    private String getInternalAccountId(String accountId, Map<String, String> orgInfo) {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        if (!StringUtils.isNotEmpty(accountId)) {
            throw new LedpException(LedpCode.LEDP_39001, new String[] { accountId, customerSpace });
        }

        String lookupIdColumn = lookupIdMappingProxy.findLookupIdColumn(orgInfo, customerSpace);

        // If AccountLookup table is populated for this tenant,
        // skip both special cache and redshift, go straight to match api
        // special cach lookup should be removed soon
        String internalAccountId;
        if (Boolean.TRUE.equals(hasAccountLookupCache.get(customerSpace))) {
            internalAccountId = matchProxy.lookupInternalAccountId(customerSpace, lookupIdColumn, accountId, null);
        } else {
            log.warn("Tenant " + customerSpace + " does not have AccountLookup table, trying special lookupcache");

            internalAccountId = getInternalIdViaAccountCache(customerSpace, lookupIdColumn, accountId);

            if (StringUtils.isBlank(internalAccountId)) {
                log.warn(String.format(
                        "Failed to find LookupId:%s AccountId:%s for customerspace: %s in special cache. attempting redshift lookup.",
                        lookupIdColumn, accountId, customerSpace));
                List<String> internalAccountIds = getInternalAccountsIdViaObjectApi(customerSpace,
                        Collections.singletonList(accountId), lookupIdColumn);
                internalAccountId = CollectionUtils.isNotEmpty(internalAccountIds) ? internalAccountIds.get(0) : null;
            }
        }
        return internalAccountId;

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
            boolean entityMatchEnabled = batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace));
            return StatsCubeUtils.constructTopNTree(cubes, cmMap, true, ColumnSelection.Predefined.Segment,
                    entityMatchEnabled);
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.DataLakeStatsCubesCache, key = "T(java.lang.String).format(\"%s|statscubes\", #customerSpace)", unless = "#result == null")
    public Map<String, StatsCube> getStatsCubesFromCache(String customerSpace) {
        StatisticsContainer container = dataCollectionProxy.getStats(customerSpace);
        if (container != null) {
            Map<String, StatsCube> cubeMap = container.getStatsCubes();
            if (MapUtils.isNotEmpty(cubeMap)) {
                cubeMap.forEach((entityName, cube) -> {
                    BusinessEntity entity = BusinessEntity.valueOf(entityName);
                    StatsCubeUtils.sortBkts(cube, entity);
                });
            }
            return cubeMap;
        }
        return null;
    }

    @VisibleForTesting
    String getInternalIdViaAccountCache(String customerSpace, String lookupIdColumn, String accountId) {
        String internalAccountId = null;
        try {
            String lookupIdKey = MessageFormat.format(DYNAMO_ACCOUNT_LOOKUP_CACHE_KEY_FORMAT,
                    CustomerSpace.parse(customerSpace).getTenantId(), lookupIdColumn, accountId.toLowerCase());
            String accountIdKey = MessageFormat.format(DYNAMO_ACCOUNT_LOOKUP_CACHE_KEY_FORMAT,
                    CustomerSpace.parse(customerSpace).getTenantId(), DYNAMO_ACCOUNT_LOOKUP_CACHE_ACCOUNTID_FIELD,
                    accountId.toLowerCase());
            List<Item> items = dynamoItemService
                    .batchGet(dynamoAccountLookupCacheTableName,
                            Arrays.asList(new PrimaryKey(DYNAMO_ACCOUNT_LOOKUP_CACHE_KEY_FIELD, accountIdKey),
                                    new PrimaryKey(DYNAMO_ACCOUNT_LOOKUP_CACHE_KEY_FIELD, lookupIdKey)))
                    .stream().filter(Objects::nonNull).collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(items)) {
                internalAccountId = (String) items.get(0).get(DYNAMO_ACCOUNT_LOOKUP_CACHE_VALUE_FIELD);
            } else {
                log.warn("Failed to lookup accountId in dynamo cache by LookupIdkey: " + lookupIdKey
                        + " and AccountIdkey: " + accountIdKey);
            }
        } catch (Exception e) {
            log.error("Failed to lookup accountId in dynamo cache due to " + e.getMessage());
        }
        return internalAccountId;
    }

    private List<String> getInternalAccountsIdViaObjectApi(String customerSpace, List<String> accountIds,
            String lookupIdColumn) {

        DataPage entityData;
        try {
            FrontEndQuery frontEndQuery = AccountExtensionUtil.constructFrontEndQuery(customerSpace, accountIds,
                    lookupIdColumn, null, true, batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)));
            log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
            entityData = entityProxy.getDataFromObjectApi(customerSpace, frontEndQuery);
        } catch (Exception e) {
            FrontEndQuery frontEndQuery = AccountExtensionUtil.constructFrontEndQuery(customerSpace, accountIds,
                    lookupIdColumn, null, false, batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace)));
            log.info(String.format("Calling entityProxy with request payload: %s", JsonUtils.serialize(frontEndQuery)));
            entityData = entityProxy.getDataFromObjectApi(customerSpace, frontEndQuery);
        }

        return AccountExtensionUtil.extractAccountIds(entityData);
    }

    private DataPage getAccountByIdViaMatchApi(String customerSpace, String internalAccountId,
            ColumnSelection.Predefined predefined) {

        String dataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        MatchInput matchInput;
        if (batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpace))) {
            matchInput = AccountExtensionUtil.constructEntityMatchInput(customerSpace,
                    Collections.singletonList(internalAccountId), predefined, dataCloudVersion);
        } else {
            matchInput = AccountExtensionUtil.constructMatchInput(customerSpace,
                    Collections.singletonList(internalAccountId), predefined, dataCloudVersion);
        }
        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);

        List<ColumnMetadata> servingMetadata = getCachedServingMetadataForEntity(customerSpace, BusinessEntity.Account);
        List<ColumnMetadata> dateAttributesMetadata = servingMetadata.stream().filter(ColumnMetadata::isDateAttribute)
                .collect(Collectors.toList());
        return AccountExtensionUtil.processMatchOutputResults(customerSpace, dateAttributesMetadata, matchOutput);
    }

    private DataPage getAccountByIdViaMatchApi(String customerSpace, String internalAccountId, List<Column> fields) {
        List<List<Object>> data = new ArrayList<>();
        List<Object> datum = new ArrayList<>();
        datum.add(internalAccountId);
        data.add(datum);

        Tenant tenant = new Tenant(customerSpace);
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(tenant);
        matchInput.setFields(LOOKUP_FIELDS);
        matchInput.setData(data);
        matchInput.setKeyMap(KEY_MAP);
        ColumnSelection customFieldSelection = new ColumnSelection();
        customFieldSelection.setColumns(fields);
        matchInput.setCustomSelection(customFieldSelection);
        String dataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
        matchInput.setUseRemoteDnB(false);
        matchInput.setDataCloudVersion(dataCloudVersion);

        log.info(String.format("Calling matchapi with request payload: %s", JsonUtils.serialize(matchInput)));
        MatchOutput matchOutput = matchProxy.matchRealTime(matchInput);
        return convertToDataPage(customerSpace, matchOutput);
    }

    private DataPage convertToDataPage(String customerSpace, MatchOutput matchOutput) {
        DataPage dataPage = createEmptyDataPage();
        Map<String, Object> data = null;
        if (matchOutput != null //
                && CollectionUtils.isNotEmpty(matchOutput.getResult()) //
                && matchOutput.getResult().get(0) != null) {

            if (matchOutput.getResult().get(0).isMatched() != Boolean.TRUE) {
                log.info("No match on MatchApi, reverting to ObjectApi on Tenant: " + customerSpace);
            } else {
                log.info("Found full match from lattice data cloud as well as from my data table.");
                final Map<String, Object> tempDataRef = new HashMap<>();
                List<String> fields = matchOutput.getOutputFields();
                List<Object> values = matchOutput.getResult().get(0).getOutput();
                IntStream.range(0, fields.size()) //
                        .forEach(i -> tempDataRef.put(fields.get(i), values.get(i)));
                data = tempDataRef;
            }
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

    private Flux<ColumnMetadata> getAllSegmentAttributes() {
        String tenantId = MultiTenantContext.getShortTenantId();
        boolean entityMatchEnabled = batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace());

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
                        Map<String, AttributeStats> statsMap = statsCube.getStatistics();
                        if (MapUtils.isEmpty(statsMap)) {
                            statsMap = Collections.emptyMap();
                        }
                        Set<String> attrsInStats = statsMap.keySet();
                        flux = flux //
                                .filter(cm -> cm.isEnabledFor(ColumnSelection.Predefined.Segment)) //
                                .filter(cm -> !StatsCubeUtils.shouldHideAttr(entity, cm, entityMatchEnabled)) //
                                .filter(cm -> {
                                    boolean avail = availableAttrs.contains(cm.getAttrName());
                                    if (!avail) {
                                        log.info("Filtering out  " + cm.getAttrName() + " in " + entity
                                                + " because it is not in serving store.");
                                    }
                                    return avail;
                                }) //
                                .filter(cm -> {
                                    boolean avail = attrsInStats.contains(cm.getAttrName());
                                    if (!avail) {
                                        log.info("Filtering out  " + cm.getAttrName() + " in " + entity
                                                + " because it is not in stats cube.");
                                    }
                                    return avail;
                                });
                        flux = StatsCubeUtils.filterByStats(flux, statsCube);
                        return StatsCubeUtils.sortByCategory(flux, statsCube);
                    }
                });
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
                        Set<String> attrs = servingStoreProxy.getServingStoreColumnsFromCache(customerSpace, entity);
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
                Map<String, StatsCube> mapCopy = new HashMap<>();
                cubes.forEach((key, cube) -> {
                    if (cube != null) {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        KryoUtils.write(bos, cube);
                        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                        StatsCube cubeCopy = KryoUtils.read(bis, StatsCube.class);
                        mapCopy.put(key, cubeCopy);
                    }
                });
                if (MapUtils.isNotEmpty(mapCopy)) {
                    concurrentStatsCubeMap.putAll(mapCopy);
                }
                log.info("Finished getting stats cubes for " + tenantId);
            } else {
                log.warn("There's no stats cubes for tenantId=" + tenantId);
            }
        };
        runnables.add(statsCubeRunnable);
        ThreadPoolUtils.runInParallel(runnables);
        metadataMap.putAll(concurrentMetadataMap);
        columnNamesMap.putAll(concurrentColumnNamesMap);
        concurrentStatsCubeMap.forEach((entity, cube)-> {
            // filter only segmentable attributes
            Set<String> cols = columnNamesMap.get(BusinessEntity.valueOf(entity));
            Map<String, AttributeStats> stats = cube.getStatistics();
            Map<String, AttributeStats> stats2 = new HashMap<>();
            stats.forEach((a, s) -> {
                if (cols.contains(a)) {
                    stats2.put(a, s);
                }
            });
            cube.setStatistics(stats2);
        });
        statsCubeMap.putAll(concurrentStatsCubeMap);
        log.info(String.format("Finished collection metadata artifacts in parallel for %s. Used %d secs.", tenantId,
                stopWatch.getTime(TimeUnit.SECONDS)));
    }

    @Override
    public List<ColumnMetadata> getCachedServingMetadataForEntity(String customerSpace, BusinessEntity entity) {
        return getServingMetadataForEntity(customerSpace, entity).collectList().block();
    }

    private Flux<ColumnMetadata> getServingMetadataForEntity(String customerSpace, BusinessEntity entity) {
        Flux<ColumnMetadata> cms = Flux.empty();
        if (entity != null) {
            List<ColumnMetadata> cmList = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, entity);
            if (CollectionUtils.isNotEmpty(cmList)) {
                cms = Flux.fromIterable(cmList).filter(cm -> !AttrState.Inactive.equals(cm.getAttrState()));
                if (BusinessEntity.Account.equals(entity)) {
                    cms = ImportanceOrderingUtils.addImportanceOrdering(cms);
                }
                AtomicLong counter = new AtomicLong();
                AtomicLong timer = new AtomicLong();
                cms = cms.doOnSubscribe(s -> timer.set(System.currentTimeMillis())) //
                        .doOnNext(cm -> counter.getAndIncrement()).doOnComplete(() -> {
                            long duration = System.currentTimeMillis() - timer.get();
                            long count = counter.get();
                            if (duration > 500) {
                                String msg = "Retrieved " + count + " attributes from serving store proxy for " + entity
                                        + " in " + customerSpace + " TimeElapsed=" + duration + " msec.";
                                log.info(msg);
                            }
                        });
            }
        }
        return cms;
    }

    @VisibleForTesting
    void setMatchProxy(MatchProxy matchProxy) {
        this.matchProxy = matchProxy;
    }

    @VisibleForTesting
    void setAccountLookupCacheTable(String testTable) {
        this.dynamoAccountLookupCacheTableName = testTable;
    }

    private boolean hasAccountLookupCache(String customerSpace) {
        DynamoDataUnit dynamoDataUnit = //
                dataCollectionProxy.getAccountLookupDynamoDataUnit(customerSpace, null);
        return dynamoDataUnit != null;
    }

    private boolean contactsByAccountLookupsPopulated(String customerSpace) {
        DynamoDataUnit dynamoDataUnit = //
                dataCollectionProxy.getContactsByAccountLookupDataUnit(customerSpace, null);
        return dynamoDataUnit != null;
    }
}
