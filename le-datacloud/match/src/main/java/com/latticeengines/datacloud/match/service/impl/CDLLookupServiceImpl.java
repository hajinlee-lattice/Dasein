package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.dynamo.DynamoItemService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.CDLLookupService;
import com.latticeengines.datafabric.entitymanager.GenericTableEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.GenericTableEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.ElasticSearchDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import reactor.core.publisher.Flux;

@Service("CDLLookupService")
public class CDLLookupServiceImpl implements CDLLookupService {

    private static final Logger log = LoggerFactory.getLogger(CDLLookupServiceImpl.class);

    private static final String ACCOUNT_LOOKUP_DATA_UNIT_NAME = "AccountLookup";
    private static final String ACCOUNT_LOOKUP_KEY_FORMAT = "AccountLookup__%s__%s__%s_%s"; // tenantId__ver__lookupId_lookIdVal
    private static final String ACCOUNT_LOOKUP_TABLE_FORMAT = "AccountLookup_%s"; // + signature
    private static final String LOOKUP_DELETED = "__Deleted__";

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private FabricDataService dataService;

    @Inject
    protected DataUnitProxy dataUnitProxy;

    @Inject
    protected DynamoItemService dynamoItemService;

    @Inject
    private ElasticSearchService elasticSearchService;

    @Inject
    private BatonService batonService;

    @Value("${eai.export.dynamo.accountlookup.signature}")
    private String accountLookupSignature;

    private final Map<String, GenericTableEntityMgr> dynamoDataStoreMap = new ConcurrentHashMap<>();

    private LoadingCache<String, DynamoDataUnit> roleBasedDynamoDataCache = Caffeine.newBuilder() //
            .maximumSize(1000) //
            .expireAfterWrite(1, TimeUnit.MINUTES) //
            .build(this::getDynamoDataUnit);

    private LoadingCache<String, List<DynamoDataUnit>> dataUnitsCache = Caffeine.newBuilder() //
            .maximumSize(500) //
            .expireAfterWrite(1, TimeUnit.MINUTES) //
            .build(this::getCustomDynamoDataUnits);

    private LoadingCache<String, DynamoDataUnit> accountLookupDUCache = Caffeine.newBuilder() //
            .maximumSize(500) //
            .expireAfterWrite(1, TimeUnit.MINUTES) //
            .build(this::getAccountLookupDataUnit);

    @Override
    public List<ColumnMetadata> parseMetadata(MatchInput input) {
        List<ColumnSelection.Predefined> predefinedList = Collections.emptyList();
        Set<String> extraColumns = Collections.emptySet();
        boolean isCustomSelection = false;
        if (input.getUnionSelection() != null) {
            if (MapUtils.isNotEmpty(input.getUnionSelection().getPredefinedSelections())) {
                predefinedList = new ArrayList<>(input.getUnionSelection().getPredefinedSelections().keySet());
            }
            if (input.getUnionSelection().getCustomSelection() != null) {
                extraColumns = extractColumnNames(input.getUnionSelection().getCustomSelection());
            }
        } else if (input.getPredefinedSelection() != null) {
            predefinedList = Collections.singletonList(input.getPredefinedSelection());
        } else {
            extraColumns = extractColumnNames(input.getCustomSelection());
            isCustomSelection = true;
        }

        String customerSpace = input.getTenant().getId();
        @SuppressWarnings("unused")
        DataCollection.Version version = input.getDataCollectionVersion();
        // TODO: get metadata by version
        List<ColumnMetadata> cms = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.ACCOUNT_MATCH_ENTITIES) {
            List<ColumnMetadata> list = servingStoreProxy.getDecoratedMetadataFromCache(customerSpace, entity);
            if (CollectionUtils.isNotEmpty(list)) {
                cms.addAll(list);
            }
        }
        boolean finalIsCustomSelection = isCustomSelection;
        Flux<ColumnMetadata> flux = Flux.fromIterable(cms) //
                .filter(cm -> finalIsCustomSelection || !AttrState.Inactive.equals(cm.getAttrState()));

        final List<ColumnSelection.Predefined> finalPredefinedList = new ArrayList<>(predefinedList);
        final Set<String> finalExtraColumns = new HashSet<>(extraColumns);
        final Set<String> outputColumnNames = new HashSet<>();
        flux = flux.filter(cm -> {
            boolean alreadyIncluded = outputColumnNames.contains(cm.getAttrName());
            boolean shouldAdd = false;
            if (!alreadyIncluded) {
                boolean inExtraColumnList = finalExtraColumns.contains(cm.getAttrName());
                boolean inPredefinedList = false;
                if (CollectionUtils.isNotEmpty(finalPredefinedList)) {
                    for (ColumnSelection.Predefined predefined : finalPredefinedList) {
                        if (cm.isEnabledFor(predefined)) {
                            inPredefinedList = true;
                            break;
                        }
                    }
                }
                shouldAdd = inExtraColumnList || inPredefinedList;
                if (shouldAdd) {
                    outputColumnNames.add(cm.getAttrName());
                }
            }
            return shouldAdd;
        });

        return flux.collectList().block();
    }

    @Override
    public DynamoDataUnit parseAccountLookupDataUnit(MatchInput input) {
        String customerSpace = input.getTenant().getId();
        DynamoDataUnit unit = accountLookupDUCache.get(customerSpace);
        DataCollection.Version version = input.getDataCollectionVersion();
        if (unit == null) {
            unit = roleBasedDynamoDataCache.get(getCacheKey(CustomerSpace.shortenCustomerSpace(customerSpace),
                    TableRoleInCollection.AccountLookup, version));
        }
        return unit;
    }

    @Override
    public List<DynamoDataUnit> parseCustomDynamoDataUnits(MatchInput input) {
        String customerSpace = input.getTenant().getId();
        DataCollection.Version version = input.getDataCollectionVersion();
        return dataUnitsCache.get(version == null ? customerSpace : customerSpace + "|" + version.toString());
    }

    @Override
    public String lookupInternalAccountId(@NotNull String customerSpace, DataCollection.Version version, String lookupIdKey, String lookupIdValue) {
        boolean enabled = batonService.isEnabled(CustomerSpace.parse(customerSpace),
                LatticeFeatureFlag.QUERY_FROM_ELASTICSEARCH);
        ElasticSearchDataUnit dataUnit;
        if (enabled && (dataUnit = (ElasticSearchDataUnit) dataUnitProxy.getByNameAndType(customerSpace,
                BusinessEntity.Account.name(),
                DataUnit.StorageType.ElasticSearch)) != null) {
            log.info("feature flag is enabled and data unit is null for {}", customerSpace);
            String signature = dataUnit.getSignature();
            String indexName = ElasticSearchUtils.constructIndexName(customerSpace, BusinessEntity.Contact.name(),
                    signature);
            return lookupInternalAccountIdByEs(customerSpace, indexName, lookupIdKey, lookupIdValue);
        } else {
            DynamoDataUnit lookupDataUnit = accountLookupDUCache.get(customerSpace);
            if (lookupDataUnit == null) {
                return legacyLookupInternalAccountId(customerSpace, version, lookupIdKey, lookupIdValue);
            }
            return getInternalAccountId(lookupDataUnit, lookupIdKey, lookupIdValue);
        }
    }

    @Override
    public List<String> lookupInternalAccountIds(String customerSpace, DataCollection.Version version, String lookupIdKey,
                                            List<String> lookupIdValues) {
        if (CollectionUtils.isEmpty(lookupIdValues)) {
            return Collections.emptyList();
        }
        DynamoDataUnit lookupDataUnit = accountLookupDUCache.get(customerSpace);
        if (lookupDataUnit == null) {
            log.info("lookupDataUnit is null, return original lookup values.");
            return lookupIdValues;
        }
        return getInternalAccountIds(lookupDataUnit, lookupIdKey, lookupIdValues);
    }

    @Override
    public boolean clearAccountLookupDUCache() {
        accountLookupDUCache.invalidateAll();
        return true;
    }

    @Override
    public String lookupInternalAccountIdByEs(@NotNull String customerSpace, @NotNull String indexName,
            @NotNull String lookupIdKey, @NotNull String lookupIdValue) {
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startSpan("lookupInternalAccountIdByEs",  start)) {
            workflowSpan = tracer.activeSpan();
            workflowSpan.log(String.format("customerspace=%s, %s:%s,%s:%s",customerSpace,
                    "lookupIdKey", lookupIdKey, "lookupIdValue", lookupIdValue));
            if (lookupIdKey == null) {
                return lookupIdValue;
            }
            String accountId = elasticSearchService.searchAccountIdByLookupId(indexName.toLowerCase(),
                    lookupIdKey, lookupIdValue);
            workflowSpan.log(String.format("accountId is %s.", accountId));
            return accountId;
        } finally {
            finish(workflowSpan);
        }
    }

    private String getInternalAccountId(DynamoDataUnit lookupDataUnit, String lookupIdKey, String rawLookupIdValue) {
        if (lookupDataUnit != null) {
            String lookupIdValue = rawLookupIdValue.toLowerCase();
            String signature = extractAccountLookupSignature(lookupDataUnit);
            String tenantId = lookupDataUnit.getTenant();
            Integer version = lookupDataUnit.getVersion();
            String tableName = String.format(ACCOUNT_LOOKUP_TABLE_FORMAT, signature);
            boolean entityMatchEnable = batonService.isEntityMatchEnabled(CustomerSpace.parse(tenantId));
            List<String> keys = getKeys(tenantId, version, lookupIdKey, lookupIdValue, entityMatchEnable);
            List<PrimaryKey> searchKeys = keys.stream()
                    .map(key -> new PrimaryKey(InterfaceName.AtlasLookupKey.name(), key)).collect(Collectors.toList());
            Map<String, Item> records = dynamoItemService.batchGet(tableName, searchKeys).stream()
                    .filter(record -> BooleanUtils.isNotTrue(record.getBoolean(LOOKUP_DELETED)))
                    .map(record -> Pair.of(record.getString(InterfaceName.AtlasLookupKey.name()), record))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            List<Item> validRecords = keys.stream().map(records::get).filter(Objects::nonNull).collect(Collectors.toList());
            if (records.size() >= 1) { // find first added to search key
                return validRecords.get(0).getString(InterfaceName.AccountId.name());
            }
        }
        return null;
    }

    private List<String> getKeys(String tenantId, Integer version, String lookupIdKey, String lookupIdValue, boolean entityMatchEnable) {
        List<String> keys = new ArrayList<>();
        if (StringUtils.isNotBlank(lookupIdKey)) {
            keys.add(constructAccountLookupKey(tenantId, version, lookupIdKey, lookupIdValue));
        }
        if (entityMatchEnable && !InterfaceName.CustomerAccountId.name().equalsIgnoreCase(lookupIdKey)) {
            keys.add(constructAccountLookupKey(tenantId, version, InterfaceName.CustomerAccountId.name(), lookupIdValue));
        }
        if (!InterfaceName.AccountId.name().equalsIgnoreCase(lookupIdKey)) {
            keys.add(constructAccountLookupKey(tenantId, version, InterfaceName.AccountId.name(), lookupIdValue));
        }
        return keys;
    }

    private List<String> getInternalAccountIds(DynamoDataUnit lookupDataUnit, String lookupIdKey, List<String> rawLookupIdValues) {
        if (lookupDataUnit != null) {
            String signature = extractAccountLookupSignature(lookupDataUnit);
            String tenantId = lookupDataUnit.getTenant();
            Integer version = lookupDataUnit.getVersion();
            String tableName = String.format(ACCOUNT_LOOKUP_TABLE_FORMAT, signature);
            Map<String, List<PrimaryKey>> searchKeys = new HashMap<>();
            boolean entityMatchEnable = batonService.isEntityMatchEnabled(CustomerSpace.parse(tenantId));
            for (String rawLookupIdValue : rawLookupIdValues) {
                String lookupIdValue = rawLookupIdValue.toLowerCase();
                List<String> keys = getKeys(tenantId, version, lookupIdKey, lookupIdValue, entityMatchEnable);
                searchKeys.put(rawLookupIdValue, keys.stream().map(key -> new PrimaryKey(InterfaceName.AtlasLookupKey.name(), key)).collect(Collectors.toList()));
            }
            Map<String, Item> records = dynamoItemService.batchGet(tableName,
                    searchKeys.values().stream().flatMap(List::stream).collect(Collectors.toList())).stream()
                    .filter(record -> BooleanUtils.isNotTrue(record.getBoolean(LOOKUP_DELETED)))
                    .map(record -> Pair.of(record.getString(InterfaceName.AtlasLookupKey.name()), record))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            return rawLookupIdValues.stream().map(rawLookupIdValue -> getInternalIdValue(rawLookupIdValue, searchKeys, records)).collect(Collectors.toList());
        } else {
            log.info("lookupDataUnit is null when query internal account id.");
            return rawLookupIdValues;
        }
    }

    private String getInternalIdValue(String rawLookupValue, Map<String, List<PrimaryKey>> searchKeys, Map<String, Item> records) {
        List<PrimaryKey> keys = searchKeys.get(rawLookupValue);
        List<Item> validRecords =
                keys.stream().map(key -> records.get(key.getComponents().iterator().next().getValue())).filter(Objects::nonNull).collect(Collectors.toList());
        if (validRecords.size() >= 1) { // find first added to search key
            return validRecords.get(0).getString(InterfaceName.AccountId.name());
        } else {
            return rawLookupValue;
        }
    }

    private String legacyLookupInternalAccountId(String customerSpace, DataCollection.Version version, String lookupIdKey,
                                          String lookupIdValue) {
        DynamoDataUnit lookupDataUnit = //
                roleBasedDynamoDataCache.get(getCacheKey(CustomerSpace.shortenCustomerSpace(customerSpace),
                        TableRoleInCollection.AccountLookup, version));
        return legacyGetInternalAccountId(lookupDataUnit, lookupIdKey, lookupIdValue);
    }

    private String legacyGetInternalAccountId(DynamoDataUnit lookupDataUnit, String lookupIdKey, String lookupIdValue) {
        if (lookupDataUnit != null) {
            String signature = lookupDataUnit.getSignature();
            GenericTableEntityMgr tableEntityMgr = getTableEntityMgr(signature);
            String tenantId = parseTenantId(lookupDataUnit);
            String tableName = parseTableName(lookupDataUnit);
            List<Pair<String, String>> keyPairs = new ArrayList<>();
            if (StringUtils.isNotBlank(lookupIdKey)) {
                keyPairs.add(Pair.of(lookupIdKey + "_" + lookupIdValue.toLowerCase(), "0"));
            }
            keyPairs.add(Pair.of(InterfaceName.AccountId.name() + "_" + lookupIdValue.toLowerCase(), "0"));
            List<Map<String, Object>> rows = tableEntityMgr.getByKeyPairs(tenantId, tableName, keyPairs);
            String accountId = null;

            for (Map<String, Object> row : rows) {
                if (row != null && StringUtils.isBlank(accountId)) {
                    accountId = row.getOrDefault(InterfaceName.AccountId.name(), "").toString();
                }
            }
            return accountId;
        } else {
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> lookupContactsByInternalAccountId(String customerSpace,
            DataCollection.Version version, String lookupIdKey, String lookupIdValue, String contactId) {
        boolean enabled = batonService.isEnabled(CustomerSpace.parse(customerSpace),
                LatticeFeatureFlag.QUERY_FROM_ELASTICSEARCH);
        ElasticSearchDataUnit dataUnit;
        if (enabled && (dataUnit = (ElasticSearchDataUnit) dataUnitProxy.getByNameAndType(customerSpace, BusinessEntity.Contact.name(),
                DataUnit.StorageType.ElasticSearch)) != null) {
            log.info("feature flag is enabled and data unit is not null for {}", customerSpace);
            String signature = dataUnit.getSignature();
            String indexName = ElasticSearchUtils.constructIndexName(customerSpace, BusinessEntity.Contact.name(),
                    signature);
            ElasticSearchDataUnit accountDataUnit =
                    (ElasticSearchDataUnit) dataUnitProxy.getByNameAndType(customerSpace,
                            BusinessEntity.Account.name(), DataUnit.StorageType.ElasticSearch);
            String accountIndexName = null;
            if (accountDataUnit != null) {
                accountIndexName = ElasticSearchUtils.constructIndexName(customerSpace, BusinessEntity.Account.name()
                        , accountDataUnit.getSignature());
            }
            return lookupContactsByESInternalAccountId(customerSpace, indexName, accountIndexName, lookupIdKey,
                    lookupIdValue,
                    contactId);
        }
        String internalAccountId = lookupInternalAccountId(customerSpace, version, lookupIdKey, lookupIdValue);

        if (StringUtils.isBlank(internalAccountId)) {
            log.info(String.format("No Account found for LookupId:%s | LookupIdValue:%s | CustomerSpace: %s",
                    lookupIdKey, lookupIdValue, customerSpace));
            return new ArrayList<>();
        }

        DynamoDataUnit consolidatedContactLookupDataUnit = roleBasedDynamoDataCache.get(getCacheKey(
                CustomerSpace.shortenCustomerSpace(customerSpace), TableRoleInCollection.ConsolidatedContact, version));
        if (consolidatedContactLookupDataUnit == null) {
            log.info(String.format("No Dynamo dataunit found for Account based contact lookups for CustomerSpace: %s",
                    customerSpace));
            return new ArrayList<>();
        }

        String signature = consolidatedContactLookupDataUnit.getSignature();
        GenericTableEntityMgr tableEntityMgr = getTableEntityMgr(signature);
        String tenantId = parseTenantId(consolidatedContactLookupDataUnit);
        String tableName = parseTableName(consolidatedContactLookupDataUnit);

        List<Map<String, Object>> contactData = StringUtils.isBlank(contactId)
                ? tableEntityMgr.getAllByPartitionKey(tenantId, tableName, internalAccountId)
                : Collections.singletonList(
                        tableEntityMgr.getByKeyPair(tenantId, tableName, Pair.of(internalAccountId, contactId)));

        DynamoDataUnit curatedContactLookupDataUnit = roleBasedDynamoDataCache
                .get(getCacheKey(CustomerSpace.shortenCustomerSpace(customerSpace),
                        TableRoleInCollection.CalculatedCuratedContact, version));
        if (curatedContactLookupDataUnit != null) {
            log.info(String.format("Curated contact data found for CustomerSpace: %s", customerSpace));
            signature = curatedContactLookupDataUnit.getSignature();
            tableEntityMgr = getTableEntityMgr(signature);
            tenantId = parseTenantId(curatedContactLookupDataUnit);
            tableName = parseTableName(curatedContactLookupDataUnit);

            List<Map<String, Object>> curatedContactData = StringUtils.isBlank(contactId)
                    ? tableEntityMgr.getAllByPartitionKey(tenantId, tableName, internalAccountId)
                    : Collections.singletonList(
                            tableEntityMgr.getByKeyPair(tenantId, tableName, Pair.of(internalAccountId, contactId)));
            return merge(contactData, curatedContactData);
        } else {
            log.info(String.format("No Dynamo dataunit found for Curated contact data for CustomerSpace: %s",
                    customerSpace));
        }

        return contactData;
    }

    @Override
    public List<Map<String, Object>> lookupContactsByESInternalAccountId(String customerSpace,
            @NotNull String indexName, String accountIndexName, String lookupIdKey, String lookupIdValue,
                                                                         String contactId) {
        List<Map<String, Object>> data = new ArrayList<>();
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startSpan("lookupContactsByESInternalAccountId",  start)) {
            workflowSpan = tracer.activeSpan();
            if (StringUtils.isNotEmpty(contactId)) {
                workflowSpan.log(String.format("%s:%s", "contactId", contactId));
                data = elasticSearchService.searchContactByContactId(indexName, contactId);
                return data;
            }
            if (lookupIdKey == null) {
                lookupIdKey = InterfaceName.AccountId.name();
            } else if (StringUtils.isEmpty(accountIndexName)) {
                log.error("accountIndexName Can't be null.");
                return data;
            }
            workflowSpan.log(String.format("customerspace=%s, %s:%s", customerSpace, "lookupIdKey",
                    lookupIdKey));
            String internalAccountId = InterfaceName.AccountId.name().equals(lookupIdKey) ? lookupIdValue
                    : lookupInternalAccountIdByEs(customerSpace, accountIndexName, lookupIdKey, lookupIdValue);
            workflowSpan.log(String.format("customerspace=%s, %s:%s", customerSpace, "internalAccountId",
                    internalAccountId));
            indexName = indexName.toLowerCase();
            if (StringUtils.isBlank(internalAccountId)) {
                log.error(String.format("No Account found for LookupId:%s | LookupIdValue:%s | CustomerSpace: %s",
                        lookupIdKey, lookupIdValue, customerSpace));
                return data;
            }
            data = elasticSearchService.searchContactByAccountId(indexName, internalAccountId);
            workflowSpan.log(String.format("attribute list number is %s.", data.size()));
            return data;
        } finally {
            finish(workflowSpan);
        }
    }

    @Override
    public List<Map<String, Object>> searchTimelineByES(@NotNull String customerSpace, String indexName,
                                                  String entity, String entityId, Long fromDate, Long toDate) {
        List<Map<String, Object>> data = new ArrayList<>();
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startSpan("searchTimelineByES",  start)) {
            workflowSpan = tracer.activeSpan();
            workflowSpan.log(String.format("customerspace=%s, %s:%s,%s:%s,%s:%s,%s:%s", customerSpace,
                    "entity", entity,"entityId", entityId,"fromDate", fromDate,"toDate", toDate));
            return elasticSearchService.searchTimelineByEntityIdAndDateRange(indexName, entity, entityId, fromDate, toDate);
        } finally {
            finish(workflowSpan);
        }
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @VisibleForTesting
    List<Map<String, Object>> merge(List<Map<String, Object>> contactData,
            List<Map<String, Object>> curatedContactData) {
        if (CollectionUtils.isEmpty(curatedContactData))
            return contactData;

        String contactId = InterfaceName.ContactId.name();
        Map<String, Map<String, Object>> mappedData = curatedContactData.stream()
                .collect(Collectors.toMap(c -> (String) c.get(contactId), c -> c));
        for (Map<String, Object> contact : contactData) {
            if (mappedData.containsKey(contact.get(contactId))) {
                contact.putAll(mappedData.get(contact.get(contactId)));
            }
        }
        return contactData;
    }

    @Override
    public Map<String, Object> lookup(DynamoDataUnit lookupDataUnit, List<DynamoDataUnit> dynamoDataUnits,
            String lookupIdKey, String lookupIdValue) {
        if (!InterfaceName.AccountId.name().equals(lookupIdKey)) {
            if (lookupDataUnit != null) {
                String accountId = ACCOUNT_LOOKUP_DATA_UNIT_NAME.equals(lookupDataUnit.getName()) ?
                        lookupInternalAccountId(lookupDataUnit.getTenant(), null, lookupIdKey, lookupIdValue) :
                        legacyGetInternalAccountId(lookupDataUnit, lookupIdKey, lookupIdValue);
                if (accountId == null) {
                    throw new RuntimeException(String.format("Failed to find account by lookupId %s=%s", //
                            lookupIdKey, lookupIdValue));
                }
            } else {
                throw new UnsupportedOperationException("Only support lookup by AccountId.");
            }
        }
        Pair<String, String> keyPair = Pair.of(lookupIdValue, "0");
        Map<String, Object> data = new HashMap<>();
        if (CollectionUtils.isEmpty(dynamoDataUnits)) {
            log.info("No dynamo data unit found for custom account.");

        } else {
            // signature -> (tenantId -> [table names])
            Map<String, Map<String, List<String>>> map = new HashMap<>();
            dynamoDataUnits.forEach(dynamoDataUnit -> {
                if (!map.containsKey(dynamoDataUnit.getSignature())) {
                    map.put(dynamoDataUnit.getSignature(), new HashMap<>());
                }
                String tenantId = parseTenantId(dynamoDataUnit);
                if (!map.get(dynamoDataUnit.getSignature()).containsKey(tenantId)) {
                    map.get(dynamoDataUnit.getSignature()).put(tenantId, new ArrayList<>());
                }
                String tableName = parseTableName(dynamoDataUnit);
                map.get(dynamoDataUnit.getSignature()).get(tenantId).add(tableName);
            });
            for (Map.Entry<String, Map<String, List<String>>> ent : map.entrySet()) {
                GenericTableEntityMgr tableEntityMgr = getTableEntityMgr(ent.getKey());
                Map<String, Object> result = tableEntityMgr.getByKeyPair(ent.getValue(), keyPair);
                if (MapUtils.isNotEmpty(result)) {
                    data.putAll(result);
                }
            }
        }
        return data;
    }

    @Override
    public Map<String, Object> lookup(@NotNull String customerSpace, String indexName,
                                      String lookupIdKey, String lookupIdValue) {
        String idxName = indexName.toLowerCase();
        Tracer tracer = GlobalTracer.get();
        Span workflowSpan = null;
        long start = System.currentTimeMillis() * 1000;
        try (Scope scope = startSpan("lookup",  start)) {
            workflowSpan = tracer.activeSpan();
            workflowSpan.log(String.format("customerspace=%s, %s:%s,%s:%s", customerSpace,
                    "lookupIdKey", lookupIdKey, "lookupIdValue", lookupIdValue));
            if (InterfaceName.AccountId.name().equals(lookupIdKey)) {
                return elasticSearchService.searchByAccountId(idxName, lookupIdValue);
            }
            return elasticSearchService.searchByLookupId(idxName, lookupIdKey, lookupIdValue);
        } finally {
            finish(workflowSpan);
        }
    }

    private String constructAccountLookupKey(String tenantId, Integer version, String lookupIdKey,
            String lookupIdValue) {
        return String.format(ACCOUNT_LOOKUP_KEY_FORMAT, tenantId, version, lookupIdKey, lookupIdValue);
    }

    private String extractAccountLookupSignature(DynamoDataUnit du) {
        return StringUtils.isBlank(du.getSignature()) ? accountLookupSignature : du.getSignature();
    }

    private String getCacheKey(String tenant, TableRoleInCollection role, DataCollection.Version version) {
        return tenant + "|" + role.name() + (version != null ? "|" + version.name() : "");
    }

    private DynamoDataUnit getDynamoDataUnit(String cacheKey) {
        String[] tokens = cacheKey.split("\\|");
        String tenant = tokens[0];
        TableRoleInCollection role = TableRoleInCollection.valueOf(tokens[1]);
        DataCollection.Version version = tokens.length > 2 ? DataCollection.Version.valueOf(tokens[2]) : null;
        return dataCollectionProxy.getDynamoDataUnitByTableRole(tenant, version, role);
    }

    private List<DynamoDataUnit> getCustomDynamoDataUnits(String cacheKey) {
        String[] tokens = cacheKey.split("\\|");
        String tenant = tokens[0];
        DataCollection.Version version = tokens.length > 1 ? DataCollection.Version.valueOf(tokens[1]) : null;
        List<TableRoleInCollection> servingTables = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.ACCOUNT_MATCH_ENTITIES) {
            TableRoleInCollection servingTable = BusinessEntity.Account.equals(entity) ? ConsolidatedAccount
                    : entity.getServingStore();
            servingTables.add(servingTable);
        }
        return dataCollectionProxy.getDynamoDataUnits(tenant, version, servingTables);
    }

    private DynamoDataUnit getAccountLookupDataUnit(String cacheKey) { // <tenant_id>
        return (DynamoDataUnit) dataUnitProxy.getByNameAndType(CustomerSpace.shortenCustomerSpace(cacheKey),
                ACCOUNT_LOOKUP_DATA_UNIT_NAME, DataUnit.StorageType.Dynamo);
    }

    private String parseTenantId(DynamoDataUnit dynamoDataUnit) {
        return StringUtils.isNotBlank(dynamoDataUnit.getLinkedTenant()) ? dynamoDataUnit.getLinkedTenant()
                : dynamoDataUnit.getTenant();
    }

    private String parseTableName(DynamoDataUnit dynamoDataUnit) {
        return StringUtils.isNotEmpty(dynamoDataUnit.getLinkedTable()) ? dynamoDataUnit.getLinkedTable()
                : dynamoDataUnit.getName();
    }

    private Set<String> extractColumnNames(ColumnSelection columnSelection) {
        Set<String> columns = new HashSet<>();
        if (CollectionUtils.isNotEmpty(columnSelection.getColumns())) {
            columnSelection.getColumns().forEach(cm -> columns.add(cm.getExternalColumnId()));
        }
        return columns;
    }

    private GenericTableEntityMgr getTableEntityMgr(String signature) {
        if (!dynamoDataStoreMap.containsKey(signature)) {
            registerTableEntityMgr(signature);
        }
        return dynamoDataStoreMap.get(signature);
    }

    private synchronized void registerTableEntityMgr(String signature) {
        GenericTableEntityMgr tableEntityMgr = new GenericTableEntityMgrImpl(dataService, signature);
        dynamoDataStoreMap.putIfAbsent(signature, tableEntityMgr);
        log.info("Registered a GenericTableEntityMgr using signature " + signature);
    }

    @VisibleForTesting
    void setServingStoreProxy(ServingStoreProxy servingStoreProxy) {
        this.servingStoreProxy = servingStoreProxy;
    }

    @VisibleForTesting
    void setDataCollectionProxy(DataCollectionProxy dataCollectionProxy) {
        this.dataCollectionProxy = dataCollectionProxy;
    }

    @VisibleForTesting
    void setDataUnitProxy(DataUnitProxy dataUnitProxy) {
        this.dataUnitProxy = dataUnitProxy;
    }

    public static Scope startSpan(String methodName, long startTime) {
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("CDLLookupServiceImpl - " + methodName) //
                .asChildOf(tracer.activeSpan())
                .withStartTimestamp(startTime) //
                .start();
        return tracer.activateSpan(span);
    }

    public static void finish(Span span) {
        if (span != null) {
            span.finish();
        }
    }
}
