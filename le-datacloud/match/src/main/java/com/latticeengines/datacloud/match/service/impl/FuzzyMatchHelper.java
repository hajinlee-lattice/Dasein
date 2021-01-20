package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.match.VboUsageConstants.EVENT_DATA;
import static com.latticeengines.domain.exposed.datacloud.match.VboUsageConstants.USAGE_EVENT_TIME_FORMAT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.service.ZkConfigurationService;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.domain.GenericFetchResult;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.datacloud.match.service.CDLLookupService;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.DirectPlusEnrichService;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.service.PrimeMetadataService;
import com.latticeengines.datacloud.match.service.TpsFetchService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.PrimeColumn;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.PrimeAccount;
import com.latticeengines.domain.exposed.datacloud.match.VboUsageEvent;
import com.latticeengines.domain.exposed.datacloud.match.config.TpsMatchConfig;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.ElasticSearchDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;

import scala.concurrent.Future;

@Component("fuzzyMatchHelper")
public class FuzzyMatchHelper implements DbHelper {

    private static final Logger log = LoggerFactory.getLogger(FuzzyMatchHelper.class);

    @Inject
    private AccountLookupService accountLookupService;

    @Resource(name = "accountMasterColumnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @Inject
    private FuzzyMatchService fuzzyMatchService;

    @Inject
    private ZkConfigurationService zkConfigurationService;

    @Inject
    private CDLLookupService cdlLookupService;

    @Inject
    private DirectPlusEnrichService directPlusEnrichService;

    @Inject
    private PrimeMetadataService primeMetadataService;

    @Inject
    private EntityMatchInternalService entityMatchInternalService;

    @Inject
    private TpsFetchService tpsFetchService;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    private ExecutorService tpsFetcher = null;

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    public MatchContext sketchExecutionPlan(MatchContext matchContext, boolean skipExecutionPlanning) {
        return matchContext;
    }

    @Override
    public void initExecutors() {
    }

    @Override
    public MatchContext fetch(MatchContext context) {
        fetchInternal(context, true);
        fetchMatchResult(context);
        return context;
    }

    @MatchStep
    private void fetchInternal(MatchContext context, boolean isSync) {
        boolean fetchOnly = context.getInput().isFetchOnly();
        if (!fetchOnly) {
            try {
                updateDecisionGraph(context.getInput());
                updateUseRemoteDnB(context.getInput());
                if (Boolean.TRUE.equals(context.isCdlLookup())) {
                    lookupCustomAccount(context);
                }
                if (isSync) {
                    fuzzyMatchService.callMatch(context.getInternalResults(), context.getInput());
                } else {
                    List<Future<Object>> futures = fuzzyMatchService.callMatchAsync(context.getInternalResults(),
                            context.getInput());
                    context.setFuturesResult(futures);
                }
            } catch (Exception e) {
                log.error("Failed to run fuzzy match.", e);
            }
        }
    }

    /*
     * Lookup lattice account id (and custom attrs) by lookup id
     */
    private void lookupCustomAccount(MatchContext context) {
        for (InternalOutputRecord record : context.getInternalResults()) {
            if (record.getCustomAccount() != null) {
                // already done lookup before
                continue;
            }
            String lookupIdKey = record.getLookupIdKey();
            String lookupIdValue = record.getLookupIdValue();
            if (StringUtils.isNotBlank(lookupIdValue)) {
                ElasticSearchDataUnit elasticSearchDataUnit = context.getElasticSearchDataUnit();
                Map<String, Object> customAccount;
                if (elasticSearchDataUnit != null) {
                    String customerSpace = context.getInput().getTenant().getId();
                    String signature = elasticSearchDataUnit.getSignature();
                    String indexName = ElasticSearchUtils.constructIndexName(customerSpace,
                            BusinessEntity.Account.name(),
                            signature);
                    customAccount = cdlLookupService.lookup(customerSpace, indexName, lookupIdKey, lookupIdValue);
                } else {
                    List<DynamoDataUnit> dynamoDataUnits = context.getCustomDataUnits();
                    DynamoDataUnit lookupDataUnit = context.getAccountLookupDataUnit();
                    customAccount =cdlLookupService.lookup(lookupDataUnit, dynamoDataUnits,
                            lookupIdKey, lookupIdValue);
                }
                if (MapUtils.isNotEmpty(customAccount)) {
                    record.setMatched(true);
                    if (InterfaceName.AccountId.name().equals(record.getLookupIdKey())) {
                        record.setEntityIds(new HashMap<>());
                        record.getEntityIds().put(BusinessEntity.Account.name(), record.getLookupIdValue());
                    }
                    record.setCustomAccount(customAccount);
                    String latticeAccountId = (String) customAccount.get(InterfaceName.LatticeAccountId.name());
                    if (StringUtils.isNotBlank(latticeAccountId)) {
                        String outputId = StringStandardizationUtils.getStandardizedOutputLatticeID(latticeAccountId);
                        record.setMatchedLatticeAccountId(outputId);
                        String inputId = StringStandardizationUtils.getStandardizedInputLatticeID(latticeAccountId);
                        record.setLatticeAccountId(inputId);
                    }
                } else {
                    record.setMatched(false);
                    String msg = "Cannot find a custom account by " + lookupIdKey + "=" + lookupIdValue;
                    record.addErrorMessages(msg);
                    log.warn(msg);
                }
            }
        }
    }

    private void updateDecisionGraph(MatchInput matchInput) {
        String decisionGraph = matchInput.getDecisionGraph();
        if (StringUtils.isEmpty(decisionGraph) && matchInput.getTenant() != null) {
            decisionGraph = defaultGraph;
        }
        matchInput.setDecisionGraph(decisionGraph);
    }

    private void updateUseRemoteDnB(MatchInput matchInput) {
        if (!zkConfigurationService.useRemoteDnBGlobal()) {
            matchInput.setUseRemoteDnB(Boolean.FALSE);
            return;
        }
        Boolean useRemoteDnB = matchInput.getUseRemoteDnB();
        if (useRemoteDnB != null) {
            return;
        }
        matchInput.setUseRemoteDnB(true);
    }

    @Override
    public MatchContext fetchAsync(MatchContext context) {
        fetchInternal(context, false);
        return context;
    }

    @Override
    public void fetchIdResult(MatchContext context) {
        try {
            fuzzyMatchService.fetchIdResult(context.getInternalResults(), context.getInput().getLogLevelEnum(),
                    context.getFuturesResult());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to process match result!", ex);
        }
    }

    /*
     * Enrichment/Append step
     */
    @MatchStep
    @Override
    public void fetchMatchResult(MatchContext context) {
        if (ContactMasterConstants.MATCH_ENTITY_TPS.equals(context.getInput().getTargetEntity())) {
            fetchTpsRecords(context);
        } else if (!context.isSeekingIdOnly()) {
            if (OperationalMode.ENTITY_MATCH.equals(context.getInput().getOperationalMode())) {
                fetchEntitySeed(context);
            } else {
                if (Boolean.TRUE.equals(context.isCdlLookup())
                        && OperationalMode.ENTITY_MATCH_ATTR_LOOKUP.equals(context.getInput().getOperationalMode())) {
                    // need to fetch lattice account id with entity id
                    updateLAIdLookupAfterEntityMatch(context);
                    lookupCustomAccount(context);
                }
                if (BusinessEntity.PrimeAccount.name().equals(context.getInput().getTargetEntity())) {
                    fetchPrimeAccount(context);
                } else {
                    fetchLatticeAccount(context);
                }
            }
        }
    }

    /*
     * After entity match, set lookup id value to entity id if it is not set by
     * user.
     */
    private void updateLAIdLookupAfterEntityMatch(@NotNull MatchContext context) {
        for (InternalOutputRecord record : context.getInternalResults()) {
            if (record.getCustomAccount() != null) {
                // skip record that already done lattice account id lookup
                continue;
            }

            if (record.getLookupIdKey() == null) {
                // set default for lookup key
                record.setLookupIdKey(InterfaceName.AccountId.name());
            }
            if (InterfaceName.AccountId.name().equals(record.getLookupIdKey())
                    && StringUtils.isBlank(record.getLookupIdValue())) {
                record.setLookupIdValue(record.getEntityId());
            }
        }
    }

    private void fetchEntitySeed(MatchContext context) {
        List<String> seedIds = context.getInternalResults().stream() //
                .map(InternalOutputRecord::getEntityId) //
                .collect(Collectors.toList());
        long startTime = System.currentTimeMillis();
        Tenant tenant = new Tenant(CustomerSpace.parse(context.getInput().getTenant().getId()).getTenantId());
        Map<EntityMatchEnvironment, Integer> versionMap = context.getInput().getEntityMatchVersionMap();
        List<EntityRawSeed> seeds = entityMatchInternalService.get(tenant, context.getInput().getTargetEntity(),
                seedIds, versionMap);
        int unmatch = 0;
        for (int i = 0; i < context.getInternalResults().size(); i++) {
            InternalOutputRecord record = context.getInternalResults().get(i);
            EntityRawSeed seed = seeds.get(i);
            if (seed == null) {
                unmatch++;
                continue;
            }
            // As long as EntityId exists, treat it as matched even if seed
            // attributes are not (fully) populated
            record.setMatched(true);
            if (CollectionUtils.isNotEmpty(seed.getLookupEntries())) {
                seed.getLookupEntries().stream() //
                        .filter(entry -> EntityLookupEntry.Type.EXTERNAL_SYSTEM.equals(entry.getType())
                                && InterfaceName.AccountId.name().equals(entry.getSerializedKeys())) //
                        .findFirst() //
                        .ifPresent(lookupEntry -> record.getQueryResult() //
                                .put(InterfaceName.AccountId.name(), lookupEntry.getSerializedValues())); //
            }
            if (MapUtils.isNotEmpty(seed.getAttributes())) {
                String latticeAccountId = seed.getAttributes().get(InterfaceName.LatticeAccountId.name());
                record.getQueryResult().put(InterfaceName.LatticeAccountId.name(), latticeAccountId);
                record.setLatticeAccountId(latticeAccountId);
            }
        }
        log.info("Fetched records from entity seed table by entity id: Total={}, Unmatched={}, Duration={}",
                context.getInternalResults().size(), unmatch, System.currentTimeMillis() - startTime);
    }

    private void fetchTpsRecords(MatchContext context) {
        Set<String> recordIds = new HashSet<>();
        for (InternalOutputRecord record : context.getInternalResults()) {
            List<String> fetchIds = record.getFetchIds();
            if (fetchIds != null) {
                recordIds.addAll(fetchIds);
            }
        }
        if (recordIds.isEmpty()) {
            return;
        }

        int numChunks = getNumOfChunks(recordIds.size());
        int size = recordIds.size() / numChunks;
        List<List<String>> chunks = ListUtils.partition(new ArrayList<>(recordIds), size);

        log.info("No. internal records = {}", context.getInternalResults().size());
        log.info("Fetching {} contacts, numChunks = {}, size = {}, chunks = {}", recordIds.size(), numChunks, size,
                chunks.stream().map(List::size).collect(Collectors.toList()));

        ExecutorService fetcher = getTpsFetcher();
        List<CompletableFuture<List<GenericFetchResult>>> futures = new ArrayList<>();
        for (List<String> chunk : chunks) {
            try (PerformanceTimer timer = //
                    new PerformanceTimer("Fetch " + chunk.size() + " tps records.")) {
                TpsMatchConfig matchConfig = context.getInput().getTpsMatchConfig();
                if (matchConfig != null) {
                    log.info("TpsMatchConfig={}", JsonUtils.serialize(matchConfig));
                }

                futures.add(CompletableFuture.supplyAsync(() -> tpsFetchService.fetchAndFilter(chunk, matchConfig),
                        fetcher));
            }
        }
        CompletableFuture<List<GenericFetchResult>> listCompletableFuture = CompletableFuture
                .allOf(futures.toArray(new CompletableFuture[0])).thenApply(v -> futures.stream()
                        .map(CompletableFuture::join).flatMap(List::stream).collect(Collectors.toList()));
        String msg = String.format("Fetching %d contacts", recordIds.size());
        try (PerformanceTimer timer = new PerformanceTimer(msg)) {
            List<GenericFetchResult> fetchResults = listCompletableFuture.get();
            log.info("Fetch result size = {}", fetchResults.size());

            for (InternalOutputRecord record : context.getInternalResults()) {
                List<String> fetchIds = record.getFetchIds();
                List<GenericFetchResult> resultsForRecord = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(fetchIds)) {
                    Set<String> fetchIdSet = new HashSet<>(fetchIds);
                    for (GenericFetchResult result : fetchResults) {
                        if (fetchIdSet.contains(result.getRecordId())) {
                            resultsForRecord.add(result.getDeepCopy());
                        }
                    }
                    record.setMultiFetchResults(resultsForRecord);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to fetch tps records in parallel", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            // testing and making sure nothing is blocked here
        }
    }

    private ExecutorService getTpsFetcher() {
        if (tpsFetcher == null) {
            synchronized (this) {
                if (tpsFetcher == null) {
                    tpsFetcher = ThreadPoolUtils.getCachedThreadPool("tps-fetcher");
                }
            }
        }

        return tpsFetcher;
    }

    private int getNumOfChunks(int recordSize) {
        int chunks = 0;
        int blocks = (int) Math.floor(recordSize / 25.0);
        if (blocks <= 1) {
            chunks = 1;
        } else if (blocks <= 1000) {
            chunks = 36;
        } else if (blocks <= 5000) {
            chunks = 48;
        } else {
            chunks = 60;
        }
        log.info("Pick {} chunks", chunks);
        return chunks;
    }

    private void fetchPrimeAccount(MatchContext context) {
        Set<String> ids = new HashSet<>();
        for (InternalOutputRecord record : context.getInternalResults()) {
            String duns = record.getPrimeDuns();
            if (record.isMatched() && StringUtils.isNotBlank(duns)) {
                ids.add(duns);
            }
        }

        List<String> elementIds = context.getColumnSelection().getColumnIds();
        // these are from baseinfo_L1_v1 block, ok to always fetch
        for (String requiredElement : Arrays.asList( //
                PrimeMetadataService.DunsNumber, //
                PrimeMetadataService.SubjectName, //
                PrimeMetadataService.SubjectCountry //
        )) {
            if (!elementIds.contains(requiredElement)) {
                elementIds.add(requiredElement);
            }
        }

        if (!elementIds.contains(PrimeMetadataService.DunsNumber)) {
            elementIds.add(PrimeMetadataService.DunsNumber);
        }

        RetryTemplate retry = RetryUtils.getRetryTemplate(5);
        List<PrimeColumn> reqColumns = retry.execute(ctx -> primeMetadataService.getPrimeColumns(elementIds));
        Map<String, List<PrimeColumn>> reqColumnsByBlockId = //
                retry.execute(ctx -> primeMetadataService.divideIntoBlocks(reqColumns));
        // these are from compnayinfo_L1_v1 block, need to be excluded from usage
        // tracking if not required by user
        List<String> extraCompanyInfoElements = new ArrayList<>();
        for (String requiredElement : Arrays.asList( //
                PrimeMetadataService.SubjectCity, //
                PrimeMetadataService.SubjectState, //
                PrimeMetadataService.SubjectState2 //
        )) {
            if (!elementIds.contains(requiredElement)) {
                extraCompanyInfoElements.add(requiredElement);
            }
        }
        Set<String> trackingBlockIds = new HashSet<>(reqColumnsByBlockId.keySet()); // for usage tracking
        if (CollectionUtils.isNotEmpty(extraCompanyInfoElements)) {
            elementIds.addAll(extraCompanyInfoElements);
            List<PrimeColumn> reqColumns2 = retry.execute(ctx -> primeMetadataService.getPrimeColumns(elementIds));
            reqColumnsByBlockId = retry.execute(ctx -> primeMetadataService.divideIntoBlocks(reqColumns2));
        }

        List<PrimeAccount> accounts;
        long start = System.currentTimeMillis();
        try (PerformanceTimer timer = //
                new PerformanceTimer("Fetch " + ids.size() + " accounts from Direct+.")) {
            List<DirectPlusEnrichRequest> requests = new ArrayList<>();
            for (String duns : ids) {
                DirectPlusEnrichRequest request = new DirectPlusEnrichRequest();
                request.setDunsNumber(duns);
                request.setReqColumnsByBlockId(reqColumnsByBlockId);
                requests.add(request);
            }
            accounts = directPlusEnrichService.fetch(requests);
        }
        long duration = System.currentTimeMillis() - start;

        Map<String, PrimeAccount> dunsAccountMap = accounts.stream() //
                .filter(pa -> pa != null && StringUtils.isNotBlank(pa.getId())) //
                .collect(Collectors.toMap(PrimeAccount::getId, pa -> pa, (duns1, duns2) -> {
                    log.warn("Found duplicated duns in fetch result: " + duns1);
                    return duns1;
                }));

        for (InternalOutputRecord record : context.getInternalResults()) {
            String duns = record.getPrimeDuns();
            if (record.isMatched() && StringUtils.isNotBlank(duns)) {
                PrimeAccount primeAccount = dunsAccountMap.get(duns);
                if (primeAccount != null) {
                    PrimeAccount copy = primeAccount.getDeepCopy();
                    record.setPrimeAccount(copy);
                    GenericFetchResult fetchResult = new GenericFetchResult();
                    fetchResult.setResult(copy.getResult());
                    fetchResult.setRecordId(copy.getId());
                    record.setFetchResult(fetchResult);
                    List<VboUsageEvent> usageEvents = parseEnrichEvents(primeAccount, trackingBlockIds);
                    if (usageEvents != null) {
                        usageEvents.forEach(e -> e.setResponseTime(duration));
                        record.addUsageEvents(usageEvents);
                    }
                }
            }
        }
    }

    private List<VboUsageEvent> parseEnrichEvents(PrimeAccount account, Collection<String> blockIds) {
        if (account.getResult().containsKey(PrimeAccount.ENRICH_ERROR_CODE)) {
            return null;
        }

        Set<String> reportBlocks = new HashSet<>();
        reportBlocks.add("baseinfo_L1_v1");
        reportBlocks.addAll(blockIds);
        String duns = account.getId();
        List<VboUsageEvent> events = new ArrayList<>();
        for (String blockId : reportBlocks) {
            VboUsageEvent event = new VboUsageEvent();
            event.setSubjectDuns(duns);
            event.setEventType(EVENT_DATA);
            event.setFeatureUri(blockId);
            event.setSubjectName((String) account.getResult().get("primaryname"));
            event.setSubjectCity((String) account.getResult().get("primaryaddr_addrlocality_name"));
            String state = (String) account.getResult().get("primaryaddr_addrregion_abbreviatedname");
            if (StringUtils.isBlank(state)) {
                state = (String) account.getResult().get("primaryaddr_addrregion_name");
            }
            event.setSubjectState(state);
            event.setSubjectCountry((String) account.getResult().get("countryisoalpha2code"));
            event.setEventTime(USAGE_EVENT_TIME_FORMAT.format(new Date()));
            events.add(event);
        }
        return events;
    }

    /*
     * Fetch lattice account attrs by lattice account id
     */
    private void fetchLatticeAccount(MatchContext context) {
        String dataCloudVersion = context.getInput().getDataCloudVersion();
        List<String> ids = new ArrayList<>();
        int notNullIds = 0;
        for (InternalOutputRecord record : context.getInternalResults()) {
            String latticeAccountId = record.getLatticeAccountId();
            ids.add(latticeAccountId);
            if (StringUtils.isNotEmpty(latticeAccountId)) {
                notNullIds++;
            }
        }
        long startTime = System.currentTimeMillis();
        List<LatticeAccount> accounts = accountLookupService.batchFetchAccounts(ids, dataCloudVersion);

        for (int i = 0; i < ids.size(); i++) {
            InternalOutputRecord record = context.getInternalResults().get(i);
            LatticeAccount account = accounts.get(i);
            record.setLatticeAccount(account);
        }

        if (notNullIds > 0) {
            log.info(String.format("Fetched %d accounts from dynamodb. Duration=%d Rows=%d", accounts.size(),
                    System.currentTimeMillis() - startTime, accounts.size()));
        }
    }

    @Override
    public MatchContext fetchSync(MatchContext context) {
        return fetch(context);
    }

    @Override
    public List<MatchContext> fetch(List<MatchContext> contexts) {
        if (contexts.isEmpty()) {
            return Collections.emptyList();
        }

        log.info("Enter executeBulk for " + contexts.size() + " match contexts.");

        String dataCloudVersion = contexts.get(0).getInput().getDataCloudVersion();
        MatchContext mergedContext = mergeContexts(contexts, dataCloudVersion);

        mergedContext = fetch(mergedContext);
        splitContext(mergedContext, contexts);

        return contexts;
    }

    @Override
    public MatchContext updateInternalResults(MatchContext context) {
        if (OperationalMode.CONTACT_MATCH.equals(context.getInput().getOperationalMode())) {
            for (InternalOutputRecord record : context.getInternalResults()) {
                updateInternalRecordByMultiFetchResults(record, context.getColumnSelection());
            }
        } else if (!context.isSeekingIdOnly()
                && !OperationalMode.ENTITY_MATCH.equals(context.getInput().getOperationalMode())) {
            for (InternalOutputRecord record : context.getInternalResults()) {
                if (BusinessEntity.PrimeAccount.name().equals(context.getInput().getTargetEntity())) {
                    updateInternalRecordByFetchResult(record, context.getColumnSelection());
                } else {
                    // FOR LDC match: Use record.latticeAccount to update
                    // record.queryResult
                    // For entity match: record.queryResult is already prepared in
                    // fetchEntityMatchResult(), so skip this part
                    updateInternalRecordByMatchedAccount(record, context.getColumnSelection(),
                            context.getInput().getDataCloudVersion());
                }
                if (record.isMatched()) {
                    setMatchedValues(record);
                }
            }
        }
        return context;
    }

    @Override
    public MatchContext mergeContexts(List<MatchContext> matchContextList, String dataCloudVersion) {
        MatchContext mergedContext = new MatchContext();
        mergedContext.setInput(matchContextList.get(0).getInput());
        mergedContext.setMatchEngine(matchContextList.get(0).getMatchEngine());
        List<InternalOutputRecord> internalOutputRecords = new ArrayList<>();
        for (MatchContext matchContext : matchContextList) {
            String contextId = UUID.randomUUID().toString();
            matchContext.setContextId(contextId);
            for (InternalOutputRecord record : matchContext.getInternalResults()) {
                record.setOriginalContextId(contextId);
                internalOutputRecords.add(record);
            }
        }
        mergedContext.setInternalResults(internalOutputRecords);
        return mergedContext;
    }

    @Override
    public void splitContext(MatchContext mergedContext, List<MatchContext> matchContextList) {
        Map<String, MatchContext> rootUidContextMap = new HashMap<>();
        for (MatchContext context : matchContextList) {
            rootUidContextMap.put(context.getContextId(), context);
            context.setInternalResults(new ArrayList<>());
        }
        for (InternalOutputRecord internalOutputRecord : mergedContext.getInternalResults()) {
            MatchContext originalContext = rootUidContextMap.get(internalOutputRecord.getOriginalContextId());
            originalContext.getInternalResults().add(internalOutputRecord);
        }
    }

    private void updateInternalRecordByMultiFetchResults(InternalOutputRecord record, ColumnSelection columnSelection) {
        List<Map<String, Object>> parsedResults = parseMultiFetchResult(record.getMultiFetchResults(), columnSelection);
        if (record.isMatched() && CollectionUtils.isEmpty(parsedResults)) {
            record.setMatched(false);
        }
        record.setMultiQueryResult(parsedResults);
    }

    private void updateInternalRecordByFetchResult(InternalOutputRecord record, ColumnSelection columnSelection) {
        Map<String, Object> queryResult = new HashMap<>();
        Map<String, Object> primeAccount = parseFetchResult(record.getFetchResult(), columnSelection);
        if (MapUtils.isNotEmpty(primeAccount)) {
            queryResult.putAll(primeAccount);
        }
        if (record.isMatched() && (record.getFetchResult() == null
                || record.getFetchResult().getResult().containsKey(PrimeAccount.ENRICH_ERROR_CODE))) {
            record.setMatched(false);
        }
        record.setQueryResult(queryResult);
    }

    private void updateInternalRecordByMatchedAccount(InternalOutputRecord record, ColumnSelection columnSelection,
            String dataCloudVersion) {
        Map<String, Object> queryResult = new HashMap<>();
        Map<String, Object> latticeAccount = parseLatticeAccount(record.getLatticeAccount(), columnSelection,
                dataCloudVersion);
        if (MapUtils.isNotEmpty(latticeAccount)) {
            queryResult.putAll(latticeAccount);
        }
        if (MapUtils.isNotEmpty(record.getCustomAccount())) {
            queryResult.putAll(record.getCustomAccount());
        }
        if (record.getLatticeAccount() != null && record.getLatticeAccount().getId() != null) {
            record.setMatched(true);
        }
        record.setQueryResult(queryResult);
    }

    private List<Map<String, Object>> parseMultiFetchResult(List<GenericFetchResult> fetchResults, //
            ColumnSelection columnSelection) {
        if (CollectionUtils.isEmpty(fetchResults)) {
            return Collections.emptyList();
        } else {
            List<Map<String, Object>> parsedList = new ArrayList<>();
            for (GenericFetchResult fetchResult : fetchResults) {
                Map<String, Object> parsed = parseFetchResult(fetchResult, columnSelection);
                if (MapUtils.isNotEmpty(parsed)) {
                    parsedList.add(parsed);
                }
            }
            return parsedList;
        }
    }

    private Map<String, Object> parseFetchResult(GenericFetchResult fetchResult, ColumnSelection columnSelection) {
        if (fetchResult == null || fetchResult.getResult().containsKey(PrimeAccount.ENRICH_ERROR_CODE)) {
            return Collections.emptyMap();
        } else {
            Map<String, Object> result = fetchResult.getResult();
            Set<String> attrsToRemove = new HashSet<>(result.keySet());
            attrsToRemove.removeAll(columnSelection.getColumnIds());
            for (String attr : attrsToRemove) {
                result.remove(attr);
            }
            return result;
        }
    }

    private Map<String, Object> parseLatticeAccount(LatticeAccount account, ColumnSelection columnSelection,
            String dataCloudVersion) {
        Map<String, Pair<BitCodeBook, List<String>>> parameters = columnSelectionService
                .getDecodeParameters(columnSelection, dataCloudVersion);
        Map<String, Object> queryResult = new HashMap<>();
        Map<String, Object> amAttributes = (account == null) ? new HashMap<>() : account.getAttributes();
        amAttributes.put(MatchConstants.LID_FIELD, (account == null) ? null : account.getId());

        Set<String> attrMask = new HashSet<>(columnSelection.getColumnIds());
        Map<String, Object> decodedAttributes = decodeAttributes(parameters, amAttributes, attrMask);
        for (Column column : columnSelection.getColumns()) {
            String columnId = column.getExternalColumnId();
            String columnName = column.getColumnName();
            if (amAttributes.containsKey(columnId)) {
                queryResult.put(columnName, amAttributes.get(columnId));
            } else {
                Object value = decodedAttributes.getOrDefault(columnId, null);
                queryResult.put(columnName, value);
            }
        }

        return queryResult;
    }

    private Map<String, Object> decodeAttributes(Map<String, Pair<BitCodeBook, List<String>>> parameters,
            Map<String, Object> amAttributes, Set<String> attrMask) {
        Map<String, Object> decodedAttributes = new HashMap<>();
        for (Map.Entry<String, Pair<BitCodeBook, List<String>>> entry : parameters.entrySet()) {
            BitCodeBook codeBook = entry.getValue().getLeft();
            List<String> decodeFields = entry.getValue().getRight();
            String encodeField = entry.getKey();
            String encodedStr = (String) amAttributes.get(encodeField);
            decodeFields.retainAll(attrMask);
            if (!decodeFields.isEmpty()) {
                decodedAttributes.putAll(codeBook.decode(encodedStr, decodeFields));
            }
        }
        return decodedAttributes;
    }

    private void setMatchedValues(InternalOutputRecord record) {
        // FIXME: enhance by PrimeAccount
        Map<String, Object> amAttributes = (record.getLatticeAccount() == null) ? new HashMap<>()
                : record.getLatticeAccount().getAttributes();
        amAttributes.put(MatchConstants.LID_FIELD,
                (record.getLatticeAccount() == null) ? null : record.getLatticeAccount().getId());

        record.setLatticeAccountId((String) amAttributes.get(MatchConstants.LID_FIELD));
        record.setMatchedDomain((String) amAttributes.get(MatchConstants.AM_DOMAIN_FIELD));
        record.setMatchedEmployeeRange((String) amAttributes.get(MatchConstants.AM_EMPLOYEE_RANGE_FIELD));
        record.setMatchedRevenueRange((String) amAttributes.get(MatchConstants.AM_REVENUE_RANGE_FIELD));
        record.setMatchedPrimaryIndustry((String) amAttributes.get(MatchConstants.AM_PRIMARY_INDUSTRY_FIELD));
        record.setMatchedSecondIndustry((String) amAttributes.get(MatchConstants.AM_SECOND_INDUSTRY_FIELD));
        record.setDomainSource((String) amAttributes.get(MatchConstants.AM_DOMAIN_SOURCE));

        String name = (String) amAttributes.get(MatchConstants.AM_NAME_FIELD);
        String city = (String) amAttributes.get(MatchConstants.AM_CITY_FIELD);
        String state = (String) amAttributes.get(MatchConstants.AM_STATE_FIELD);
        String country = (String) amAttributes.get(MatchConstants.AM_COUNTRY_FIELD);
        String zipCode = (String) amAttributes.get(MatchConstants.AM_ZIPCODE_FIELD);
        String phone = (String) amAttributes.get(MatchConstants.AM_PHONE_NUM_FIELD);

        NameLocation nameLocation = new NameLocation();
        nameLocation.setName(name);
        nameLocation.setCity(city);
        nameLocation.setState(state);
        nameLocation.setCountry(country);
        nameLocation.setZipcode(zipCode);
        nameLocation.setPhoneNumber(phone);
        record.setMatchedNameLocation(nameLocation);
        record.setMatchedDuns((String) amAttributes.get(MatchConstants.AM_DUNS_FIELD));
        record.setMatchedDduns((String) amAttributes.get(MatchConstants.AM_DDUNS_FIELD));
    }
}
