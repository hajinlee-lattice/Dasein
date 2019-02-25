package com.latticeengines.datacloud.match.service.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.PatchBookEntityMgr;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.core.util.PatchBookUtils;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.DunsGuideBookService;
import com.latticeengines.datacloud.match.exposed.service.PatchService;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateResponse;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchLog;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchLogFile;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchMode;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("patchService")
public class PatchServiceImpl implements PatchService {

    private static final String BOOK_SOURCE_PATCHER = "Patcher";
    private static final String PATCH_LOG_KEY_MODE = "Mode";
    private static final String PATCH_LOG_KEY_TYPE = "Type";
    private static final String PATCH_LOG_KEY_DRY_RUN = "DryRun";
    private static final String PATCH_LOG_KEY_START_AT = "StartedAt";
    private static final String PATCH_LOG_KEY_UPLOAD_AT = "UploadedAt";
    private static final String PATCH_LOG_KEY_LOGS = "Logs";
    // should not be greater than 7 day
    // leave 2 hour buffer to prevent exceeding limit
    private static final int PATCH_LOG_FILE_URL_EXPIRES_IN_HOURS = 7 * 24 - 2;

    private static final Logger log = LoggerFactory.getLogger(PatchServiceImpl.class);

    @Value("${datacloud.match.realtime.max.input:1000}")
    private int maxRealTimeInput;

    @Value("${datacloud.patcher.log.s3bucket}")
    private String patchLogS3Bucket;

    @Value("${datacloud.patcher.log.dir}")
    private String patchLogDir;

    @Inject
    private AccountLookupService accountLookupService;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Inject
    private RealTimeMatchService realTimeMatchService;

    @Inject
    private PublicDomainService publicDomainService;

    @Inject
    private DnBCacheService dnBCacheService;

    @Inject
    private DunsGuideBookService dunsGuideBookService;

    @Inject
    private PatchBookEntityMgr patchBookEntityMgr;

    @Inject
    private S3Service s3Service;

    private String currentVersion;

    @PostConstruct
    public void postConstruct() {
        currentVersion = versionEntityMgr.currentApprovedVersion().getVersion();
    }

    @Override
    public List<PatchLog> lookupPatch(
            @NotNull List<PatchBook> books, @NotNull String dataCloudVersion, @NotNull PatchMode mode, boolean dryRun) {
        Preconditions.checkNotNull(books);
        Preconditions.checkNotNull(dataCloudVersion);
        Preconditions.checkNotNull(mode);

        // filter non-null book && hotfix item IF in hotfix mode
        books = books.stream()
                .filter(book -> book != null && (mode == PatchMode.Normal || book.isHotFix()))
                .collect(Collectors.toList());

        Date now = new Date();
        // separate active entries and inactive entries
        List<PatchBook> inactiveBooks = books
                .stream()
                .filter(book -> PatchBookUtils.isEndOfLife(book, now))
                .collect(Collectors.toList());
        List<PatchBook> activeBooks = books
                .stream()
                .filter(book -> !PatchBookUtils.isEndOfLife(book, now))
                .collect(Collectors.toList());

        log.info("Start to patch lookup entries with mode = {}, dataCloudVersion = {}, dryRun = {}",
                mode, dataCloudVersion, dryRun);
        log.info("# of active PatchBook entries = {}, # of inactive PatchBook entries = {}",
                activeBooks.size(), inactiveBooks.size());

        List<PatchLog> logs = new ArrayList<>();
        logs.addAll(generateLogForInactiveBooks(inactiveBooks));
        logs.addAll(patchAMLookupTable(activeBooks, dataCloudVersion, dryRun));
        logs.addAll(patchDunsGuideBookTable(activeBooks, dataCloudVersion, dryRun));

        if (!dryRun) {
            // update EOF flags
            updateEOF(inactiveBooks, activeBooks);
            // update hotfix flag (if in HotFix mode)
            updateHotFix(mode, logs);
            // update effectiveSinceVersion & expireAfterVersion (if necessary)
            updateEffectiveSinceVersion(dataCloudVersion, activeBooks);
            updateExpireAfterVersion(dataCloudVersion, inactiveBooks, activeBooks);
        }

        // sort by patch book PID in ascending order
        // no need to worry about overflow, as other places will reach bottleneck long before overflow happens
        logs.sort((l1, l2) -> (int) (l1.getPatchBookId() - l2.getPatchBookId()));
        return logs;
    }

    @Override
    public PatchLogFile uploadPatchLog(
            @NotNull PatchMode mode, @NotNull PatchBook.Type type, boolean dryRun,
            @NotNull String dataCloudVersion, @NotNull Date startAt, @NotNull List<PatchLog> patchLogs) {
        Preconditions.checkNotNull(mode);
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(dataCloudVersion);
        Preconditions.checkNotNull(startAt);
        Preconditions.checkNotNull(patchLogs);

        // construct log body
        Date uploadedAt = new Date();
        Map<String, Object> logWrapper = new HashMap<>();
        logWrapper.put(PATCH_LOG_KEY_MODE, mode);
        logWrapper.put(PATCH_LOG_KEY_TYPE, type);
        logWrapper.put(PATCH_LOG_KEY_DRY_RUN, dryRun);
        logWrapper.put(PATCH_LOG_KEY_START_AT, startAt.toString());
        logWrapper.put(PATCH_LOG_KEY_UPLOAD_AT, uploadedAt.toString());
        logWrapper.put(PATCH_LOG_KEY_LOGS, patchLogs);

        // serialize log body in memory and upload
        // NOTE change to local file implementation if memory ever becomes a problem
        String logStr = JsonUtils.serialize(logWrapper);
        InputStream is = new ByteArrayInputStream(logStr.getBytes());

        // generate s3 key
        String key = getPatchLogS3Key(mode, type, dryRun, dataCloudVersion, startAt);
        // url expires at this date
        Date urlExpiredAt = DateUtils.addHours(new Date(), PATCH_LOG_FILE_URL_EXPIRES_IN_HOURS);
        // generate public url for downloading the log file
        String url = s3Service.generateReadUrl(patchLogS3Bucket, key, urlExpiredAt).toString();
        // upload in background
        s3Service.uploadInputStream(patchLogS3Bucket, key, is, false);

        return new PatchLogFile(key, url, uploadedAt, urlExpiredAt);
    }

    @Override
    public LookupUpdateResponse patch(List<LookupUpdateRequest> updateRequests) {
        LookupUpdateResponse response = new LookupUpdateResponse();
        List<LookupUpdateResponse.Result> results = new ArrayList<>();
        for (LookupUpdateRequest request : updateRequests) {
            LookupUpdateResponse.Result result = new LookupUpdateResponse.Result();
            result.setRequest(request);
            try {
                patch(request);
                result.setSuccess(true);
            } catch (Exception e) {
                result.setSuccess(false);
                result.setErrorMessage(e.getMessage());
            }
            results.add(result);
        }
        response.setResults(results);
        return response;
    }

    /*
     * patch all entries that requires updating AMLookup table, will set the entry to the lattice account ID associated
     * with the domain + DUNS given by patchItems (override existing entry or create one if no existing entry)
     */
    @VisibleForTesting
    List<PatchLog> patchAMLookupTable(
            @NotNull List<PatchBook> activeBooks, @NotNull String dataCloudVersion, boolean dryRun) {
        final List<PatchBook> books = activeBooks
                .stream().filter(PatchBookUtils::shouldPatchAMLookupTable).collect(Collectors.toList());
        if (books.isEmpty()) {
            return Collections.emptyList();
        }

        log.info("Start patching AMLookup table, # of entries = {}", books.size());

        AccountLookupRequest req = getAccountLookupRequest(books, dataCloudVersion);

        // if batch lookup fail, considered a critical error and fail the entire API
        List<AccountLookupEntry> accountLookupEntries = accountLookupService.batchLookup(req);
        Preconditions.checkArgument(
                accountLookupEntries.size() == books.size() * 2,
                "# of AMLookupEntries should be 2 * books.size()");
        // TODO do batch update if there are performance problem (need to modify accountLookupService)
        return IntStream.range(0, books.size()).mapToObj(idx -> {
            PatchBook book = books.get(idx);
            // entry associated with input match key
            AccountLookupEntry targetEntry = accountLookupEntries.get(2 * idx);
            // entry associated with Domain + DUNS from patch items
            AccountLookupEntry patchEntry = accountLookupEntries.get(2 * idx + 1);
            String targetLatticeId = targetEntry == null ? null : targetEntry.getLatticeAccountId();
            String patchLatticeId = patchEntry == null ? null : patchEntry.getLatticeAccountId();
            // add retrieved lattice account ID to patch items
            book.getPatchItems().put(DataCloudConstants.LATTICE_ACCOUNT_ID, patchLatticeId);

            PatchLog patchLog = PatchBookUtils.newPatchLog(book);
            patchLog.setOriginalValue(Collections.singletonMap(DataCloudConstants.LATTICE_ACCOUNT_ID, targetLatticeId));
            patchLog.setStatus(PatchStatus.Patched);
            if (StringUtils.isBlank(patchLatticeId) || patchLatticeId.equals(targetLatticeId)) {
                // no need to update
                configureNoopPatchLog(patchLog, book, patchLatticeId);
            } else {
                // set log message
                patchLog.setMessage(String.format("Patched to lattice account ID = %s successfully", patchLatticeId));
                if (!dryRun) {
                    // TODO probably should not update in map*
                    if (targetEntry == null) {
                        targetEntry = new AccountLookupEntry();
                        // NOTE currently, location AM lookup patch only support Domain + Country &
                        // Domain + Country + State & Domain + Country + ZipCode so use country to
                        // check if location are required to build key

                        // NOTE have to set domain/DUNS before building ID because these fields also build ID internally.
                        targetEntry.setDomain(book.getDomain());
                        targetEntry.setDuns(book.getDuns());
                        if (StringUtils.isNotBlank(book.getCountry())) {
                            targetEntry.setId(AccountLookupEntry.buildIdWithLocation(book.getDomain(), book.getDuns(),
                                    book.getCountry(), book.getState(), book.getZipcode()));
                        } else {
                            targetEntry.setId(AccountLookupEntry.buildId(book.getDomain(), book.getDuns()));
                        }
                    }
                    updatePatchedAccountLookupEntry(patchLog, book, dataCloudVersion, patchLatticeId, targetEntry);
                }
            }

            return patchLog;
        }).collect(Collectors.toList());
    }

    /*
     * generate account lookup request to retrieve corresponding entries for each PatchBook entry's
     * input match key and patch items.
     *
     * Two lookup pairs will be added for each patch book entry, one for input match key, the other for patchItems.
     * Resulting request contains lookup pairs as follows:
     * [ InputMatchKey#1, PatchItems#1, InputMatchKey#2, PatchItems#2, ... ]
     */
    private AccountLookupRequest getAccountLookupRequest(
            @NotNull List<PatchBook> books, @NotNull String dataCloudVersion) {
        final AccountLookupRequest req = new AccountLookupRequest(dataCloudVersion);
        books.forEach(book -> {
            // NOTE currently, location AM lookup patch only support Domain + Country & Domain + Country + State &
            // Domain + Country + Zipcode
            // so use country to check if location are required to build key
            if (StringUtils.isNotBlank(book.getCountry())) {
                req.addLookupPair(book.getDomain(), book.getDuns(),
                        book.getCountry(), book.getState(), book.getZipcode());
            } else {
                req.addLookupPair(book.getDomain(), book.getDuns());
            }

            // patch items
            // NOTE currently, only support domain + DUNS
            req.addLookupPair(PatchBookUtils.getPatchDomain(book), PatchBookUtils.getPatchDuns(book));
        });
        return req;
    }

    private void configureNoopPatchLog(
            @NotNull PatchLog patchLog, @NotNull PatchBook book, @NotNull String patchLatticeId) {
        patchLog.setStatus(PatchStatus.Noop);
        String patchedDomain = PatchBookUtils.getPatchDomain(book);
        String patchDuns = PatchBookUtils.getPatchDuns(book);
        if (StringUtils.isBlank(patchLatticeId)) {
            patchLog.setMessage(String.format(
                    "No lattice account ID found by Domain = %s, DUNS = %s",
                    patchedDomain, patchDuns));
        } else {
            patchLog.setMessage(String.format(
                    "Lattice account ID = %s is not changed after patching", patchLatticeId));
        }
    }

    private void updatePatchedAccountLookupEntry(
            @NotNull PatchLog patchLog, @NotNull PatchBook book, @NotNull String dataCloudVersion,
            @NotNull String patchLatticeId, @NotNull AccountLookupEntry targetEntry) {
        // update lookup entry
        targetEntry.setLatticeAccountId(patchLatticeId);
        targetEntry.setPatched(true);
        try {
            accountLookupService.updateLookupEntry(targetEntry, dataCloudVersion);
        } catch (Exception e) {
            log.error("Failed to update AccountLookupEntry(DUNS={},Domain={},LatticeAccountId={}), error={}",
                    targetEntry.getDuns(), targetEntry.getDomain(), targetEntry.getLatticeAccountId(), e);
            patchLog.setStatus(PatchStatus.Failed);
            patchLog.setMessage(e.getMessage());
        }
    }

    /*
     * Patch all the entries that requires updating DunsGuideBook table
     */
    private List<PatchLog> patchDunsGuideBookTable(
            @NotNull List<PatchBook> books, @NotNull String dataCloudVersion, boolean dryRun) {
        // currently only two type of patching (AMLookup & DunsGuideBook)
        // NOT AMLookup means DunsGuideBook patching
        books = books.stream()
                .filter(book -> !PatchBookUtils.shouldPatchAMLookupTable(book)).collect(Collectors.toList());
        if (books.isEmpty()) {
            return Collections.emptyList();
        }

        log.info("Start patching DunsGuideBook table, # of entries = {}", books.size());

        // match and lookup corresponding duns guide book
        Map<Long, Pair<String, DunsGuideBook>> targetGuideBooks = lookupTargetDunsGuideBooks(books, dataCloudVersion);
        // lookup DUNS in patchedItems
        Map<String, Boolean> dunsInAMMap = lookupDunsInAM(books, dataCloudVersion);

        // process conflict map
        // patchBookId -> conflict group ID (entries in the same group have conflict with each other)
        Map<Long, Integer> conflictGroupMap = getConflictGroupMap(books, targetGuideBooks);
        if (!conflictGroupMap.isEmpty()) {
            log.info("# of PatchBook entries that have conflict = {}", conflictGroupMap.size());
        }

        // generate patch logs
        List<PatchLog> patchLogs = books
                .stream()
                .map(book -> generateLogForDunsGuideBookPatch(book, targetGuideBooks, dunsInAMMap, conflictGroupMap))
                .collect(Collectors.toList());

        // PatchBookId => patchDuns
        Map<Long, String> patchDunsMap = books
                .stream()
                .map(book -> Pair.of(book.getPid(), PatchBookUtils.getPatchDuns(book)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        // PatchBookId => cleanup flag
        Map<Long, Boolean> cleanupFlagMap = books
                .stream()
                .map(book -> Pair.of(book.getPid(), book.isCleanup()))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        // entries that need to be patched
        List<PatchLog> updateLogs = patchLogs
                .stream()
                .filter(patchLog -> patchLog.getStatus() == PatchStatus.Patched)
                .collect(Collectors.toList());
        updatePatchedDunsGuideBookEntry(updateLogs, dataCloudVersion, dryRun,
                targetGuideBooks, patchDunsMap, cleanupFlagMap);

        return patchLogs;
    }

    /*
     * Retrieve DunsGuideBook.Item that has the given key partition, return null if not found
     */
    private DunsGuideBook.Item getTargetItemByKeyPartition(Pair<String, DunsGuideBook> pair, String keyPartition) {
        if (pair == null || pair.getValue() == null) {
            return null;
        }

        DunsGuideBook book = pair.getValue();
        if (book.getItems() != null) {
            for (DunsGuideBook.Item item : book.getItems()) {
                if (item == null || StringUtils.isBlank(item.getDuns())) {
                    continue;
                }
                if (keyPartition.equals(item.getKeyPartition())) {
                    return item;
                }
            }
        }

        return null;
    }

    /*
     * Supply all input match keys to matcher and use the matched DUNS (if any) to lookup DunsGuideBook entries.
     *
     * Returns a map: patchBookId => [ DUNS, DunsGuideBook]
     * where
     * (a) DUNS != null && DunsGuideBook == null means has match result but no corresponding DunsGuideBook entry
     * (b) entry not in map means no matched DUNS
     * (c) if entry1 & entry2 has the same matchedDUNS, the DunsGuideBook "instance" will be the same (same java object)
     */
    @VisibleForTesting
    Map<Long, Pair<String, DunsGuideBook>> lookupTargetDunsGuideBooks(
            @NotNull List<PatchBook> books, @NotNull String dataCloudVersion) {
        List<MatchKeyTuple> inputMatchKeys = books
                .stream()
                .map(PatchBookUtils::getMatchKeyValues)
                .collect(Collectors.toList());
        List<String> matchedDunsList = matchDuns(inputMatchKeys, dataCloudVersion);
        Preconditions.checkNotNull(matchedDunsList);
        Preconditions.checkArgument(matchedDunsList.size() == books.size());

        // patchBookId => matched DUNS
        Map<Long, String> matchedDunsMap = IntStream.range(0, books.size())
                .filter(idx -> matchedDunsList.get(idx) != null)
                .mapToObj(idx -> Pair.of(books.get(idx).getPid(), matchedDunsList.get(idx)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        // TODO add batching and retry if necessary
        // DUNS => DunsGuideBook
        Map<String, DunsGuideBook> guideBooks = dunsGuideBookService
                .get(dataCloudVersion, matchedDunsList.stream().filter(Objects::nonNull).collect(Collectors.toList()))
                .stream()
                .filter(Objects::nonNull)
                // ignore duplicates
                .collect(Collectors.toMap(DunsGuideBook::getId, book -> book, (b1, b2) -> b1));

        return books.stream().map(book -> {
            if (!matchedDunsMap.containsKey(book.getPid())) {
                // no matched DUNS
                return null;
            }

            String duns = matchedDunsMap.get(book.getPid());
            return Triple.of(book.getPid(), duns, guideBooks.get(duns));
        }) // patchBookId, matchedDuns, DunsGuideBook
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(Triple::getLeft, triple -> Pair.of(triple.getMiddle(), triple.getRight())));
    }

    /*
     * Supply a list of input match key to matcher and retrieve a list of matched DUNS (null means no match)
     *
     * Batching is required due to the limit on MatchInput for real time match
     */
    private List<String> matchDuns(@NotNull List<MatchKeyTuple> tuples, @NotNull String dataCloudVersion) {
        final int batchSize = maxRealTimeInput;
        return IntStream.range(0, (tuples.size() + batchSize - 1) / batchSize)
                .mapToObj(i -> tuples.subList(i * batchSize, Math.min(tuples.size(), (i + 1) * batchSize)))
                .map(currentBatch -> Pair.of(currentBatch, newMatchInput(currentBatch, dataCloudVersion)))
                .flatMap(pair -> {
                    // NOTE flatMap preserve the order
                    // TODO add retry
                    List<MatchKeyTuple> currentTuples = pair.getKey();
                    MatchInput matchInput = pair.getValue();
                    try {
                        MatchOutput matchOutput = realTimeMatchService.match(matchInput);
                        List<OutputRecord> records = matchOutput.getResult();
                        Preconditions.checkNotNull(records);
                        Preconditions.checkArgument(records.size() == currentTuples.size());
                        return records.stream().map(record ->
                                (record == null || StringUtils.isBlank(record.getMatchedDuns()))
                                ? null
                                : record.getMatchedDuns());
                    } catch (Exception e) {
                        log.error("Failed to perform realtime match, current batch size = {}, error = {}",
                                currentTuples.size(), e);
                        // a stream that generates tuples.size() nulls
                        return Stream.<String>generate(() -> null).limit(currentTuples.size());
                    }
                })
                .collect(Collectors.toList());
    }

    /*
     * Generate MatchInput for a list of input match key, disable DnB cache to get the latest behavior from DnB
     *
     * [ Domain, Name, Country, State, City, Zipcode ] fields will be used in match input.
     * Fields not intended to be used for matching should be null in the input match key tuple
     */
    private MatchInput newMatchInput(@NotNull List<MatchKeyTuple> tuples, @NotNull String dataCloudVersion) {
        Preconditions.checkNotNull(tuples);
        Preconditions.checkArgument(tuples.size() <= maxRealTimeInput);

        // TODO use default decision graph?
        final MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        // disable DnB cache to get the most recent behavior
        matchInput.setUseDnBCache(false);
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogLevelEnum(Level.ERROR);
        matchInput.setDataCloudVersion(dataCloudVersion);
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);

        // name location fields
        matchInput.setFields(Arrays.asList(
                MatchKey.Domain.name(), MatchKey.Name.name(), MatchKey.Country.name(),
                MatchKey.State.name(), MatchKey.City.name(), MatchKey.Zipcode.name()));
        List<List<Object>> data = tuples
                .stream()
                .map(tuple -> Arrays.<Object>asList(
                        tuple.getDomain(), tuple.getName(), tuple.getCountry(),
                        tuple.getState(), tuple.getCity(), tuple.getZipcode()))
                .collect(Collectors.toList());
        matchInput.setData(data);
        return matchInput;
    }

    /*
     * Retrieve all the AccountLookup entries associated with DUNS in patchItems. If no entry found for a patch DUNS,
     * it means this DUNS does not exist in DataCloud and will not be patched.
     */
    private Map<String, Boolean> lookupDunsInAM(@NotNull List<PatchBook> books, @NotNull String dataCloudVersion) {
        final AccountLookupRequest req = new AccountLookupRequest(dataCloudVersion);
        books.forEach(book -> req.addLookupPair(null, PatchBookUtils.getPatchDuns(book)));
        // if batch lookup fail, considered a critical error and fail the entire API
        List<String> latticeAccountIds = accountLookupService.batchLookupIds(req);
        Preconditions.checkArgument(books.size() == latticeAccountIds.size());
        return IntStream.range(0, books.size()).mapToObj(idx -> {
            PatchBook book = books.get(idx);
            String patchDuns = PatchBookUtils.getPatchDuns(book);
            // whether the DUNS is in the DataCloud
            boolean isDunsInAM = latticeAccountIds.get(idx) != null;
            return Pair.of(patchDuns, isDunsInAM);
            // ignore duplicate value as the result should be the same
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));
    }

    /*
     * Extract all conflict entries where matched DUNS and key partition are the same but patch DUNS
     * are different (no way of knowing which to choose).
     *
     * Returns a map: PatchBookId => ConflictGroupId
     * where conflict group ID is used to identify which entries have conflict with each other
     *
     * NOTE some part are not functional because it is complicated, fix later if necessary
     */
    @VisibleForTesting
    Map<Long, Integer> getConflictGroupMap(
            @NotNull List<PatchBook> books, @NotNull Map<Long, Pair<String, DunsGuideBook>> targetGuideBooks) {
        // [matched DUNS + key partition] => patch DUNS => set of PatchBookIds
        Map<String, Map<String, Set<Long>>> map = new HashMap<>();
        for (PatchBook book : books) {
            Pair<String, DunsGuideBook> target = targetGuideBooks.get(book.getPid());
            String matchedDuns = target == null ? null : target.getKey();
            String patchDuns = PatchBookUtils.getPatchDuns(book);
            if (matchedDuns == null || patchDuns == null) {
                continue;
            }

            String keyPartition = MatchKeyUtils.evalKeyPartition(PatchBookUtils.getMatchKeyValues(book));
            // matchedDuns (srcDuns) + keyPartition as the unique key
            String key = keyPartition + matchedDuns;
            map.putIfAbsent(key, new HashMap<>());
            map.get(key).putIfAbsent(patchDuns, new HashSet<>());
            map.get(key).get(patchDuns).add(book.getPid());
        }

        List<Set<Long>> conflictGroupList = map
                .entrySet()
                .stream()
                // same unique key has more than 1 patchDuns
                .filter(e -> e.getValue().size() > 1)
                // merge all PatchBookIds in the same group
                .map(e -> e.getValue()
                        // a collection of Set<PatchBookId>
                        .values()
                        // stream of Set<PatchBookId>
                        .stream()
                        // turns each Set<PatchBookId> into Stream<PatchBookId> and merge
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet()))
                .collect(Collectors.toList());
        if (conflictGroupList.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Long, Integer> conflictGroupMap = new HashMap<>();
        for (int i = 0; i < conflictGroupList.size(); i++) {
            for (Long pId : conflictGroupList.get(i)) {
                conflictGroupMap.put(pId, i);
            }
        }
        return conflictGroupMap;
    }

    /*
     * Generate one PatchLog for input PatchBook entry. There are following scenarios
     * (a) Noop if this entry has conflict with other entries
     * (b) Noop if no matched DUNS from matcher for this entry
     * (c) Noop if patch DUNS does not exist in DataCloud
     * (d) Noop if cleanup = 1 and no DunsGuideBook.Item found for [ matched DUNS + key partition ]
     * (e) Patched otherwise, means that this entry will be patched later
     */
    private PatchLog generateLogForDunsGuideBookPatch(
            @NotNull PatchBook book, @NotNull Map<Long, Pair<String, DunsGuideBook>> targetGuideBooks,
            @NotNull Map<String, Boolean> dunsInAMMap, @NotNull Map<Long, Integer> conflictGroupMap) {
        PatchLog patchLog = PatchBookUtils.newPatchLog(book);
        patchLog.setStatus(PatchStatus.Patched);
        Pair<String, DunsGuideBook> target = targetGuideBooks.get(book.getPid());
        String matchedDuns = target == null ? null : target.getKey();
        String patchDuns = PatchBookUtils.getPatchDuns(book);
        String keyPartition = MatchKeyUtils.evalKeyPartition(patchLog.getInputMatchKey());
        if (target != null) {
            // [ matched DUNS, DunsGuideBook ]
            patchLog.setOriginalValue(Arrays.asList(target.getKey(), target.getValue()));
        }

        // targetDuns associated with srcDuns = matchedDuns & keyPartition
        DunsGuideBook.Item targetItem = getTargetItemByKeyPartition(target, keyPartition);
        String currentTargetDuns = targetItem == null ? null : targetItem.getDuns();
        if (conflictGroupMap.containsKey(book.getPid())) {
            // patch DUNS has conflict with other entries
            String msg = String.format(
                    "MatchedDUNS = %s resulting in conflict with other PatchBook entries, " +
                            "conflict group ID = %d", matchedDuns, conflictGroupMap.get(book.getPid()));
            patchLog.setStatus(PatchStatus.Noop);
            patchLog.setMessage(msg);
        } else if (!targetGuideBooks.containsKey(book.getPid())) {
            // no DUNS matching input match key
            patchLog.setStatus(PatchStatus.Noop);
            patchLog.setMessage("No DUNS found with the input match key");
        } else if (!Boolean.TRUE.equals(dunsInAMMap.get(patchDuns))) {
            // patch DUNS does not exist in DataCloud
            String msg = String.format("DUNS = %s in patchItems does not exist in DataCloud", patchDuns);
            patchLog.setStatus(PatchStatus.Noop);
            patchLog.setMessage(msg);
        } else if (book.isCleanup() && targetItem == null) {
            // nothing to cleanup
            String msg = String.format(
                    "No DunsGuideBook.Item found with [MatchedDUNS = %s, KeyPartition = %s]",
                    matchedDuns, keyPartition);
            patchLog.setStatus(PatchStatus.Noop);
            patchLog.setMessage(msg);
        } else if (!book.isCleanup() && patchDuns.equals(currentTargetDuns)) {
            // current target duns is the same as the one we are going to patch
            String msg = String.format(
                    "Current DUNS for [MatchedDuns = %s, KeyPartition = %s] " +
                    "is the same as DUNS = %s in patchItems",
                    matchedDuns, keyPartition, patchDuns);
            patchLog.setStatus(PatchStatus.Noop);
            patchLog.setMessage(msg);
        } else {
            // will patch this entry
            String msg = book.isCleanup()
                    ? String.format(
                            "Cleanup DunsGuideBook.Item for MatchedDUNS = %s, KeyPartition = %s successfully",
                            matchedDuns, keyPartition)
                    : String.format(
                            "Patched to DUNS = %s for MatchedDUNS = %s, KeyPartition = %s successfully",
                            patchDuns, matchedDuns, keyPartition);
            patchLog.setMessage(msg);
        }

        return patchLog;
    }

    /*
     * Update ALL entries associated with the given list of patch logs. All entries passed in are assumed valid, which
     * means (1) has matchedDuns, (2) patch DUNS exist in DataCloud (3) if it is cleanup patch, there exists a
     * DunsGuideBook.Item with corresponding KeyPartition.
     */
    @VisibleForTesting
    void updatePatchedDunsGuideBookEntry(
            @NotNull List<PatchLog> logs, @NotNull String dataCloudVersion, boolean dryRun,
            @NotNull Map<Long, Pair<String, DunsGuideBook>> targetGuideBooks, @NotNull Map<Long, String> patchDunsMap,
            @NotNull Map<Long, Boolean> cleanupFlagMap) {
        if (dryRun || logs.isEmpty()) {
            // no need to update
            return;
        }

        Set<String> dunsWithNoGuideBookEntry = targetGuideBooks
                .values()
                .stream()
                .filter(pair -> pair.getValue() == null)
                .map(Pair::getKey)
                .collect(Collectors.toSet());
        // map for entries not in DunsGuideBook table so that we get the same instance for the same srcDuns
        Map<String, DunsGuideBook> dunsGuideBookMap = dunsWithNoGuideBookEntry
                .stream()
                .map(duns -> {
                    // create DunsGuideBook entry
                    DunsGuideBook dunsGuideBook = new DunsGuideBook();
                    dunsGuideBook.setPatched(true);
                    // matched DUNS (srcDuns)
                    dunsGuideBook.setId(duns);
                    return dunsGuideBook;
                })
                .collect(Collectors.toMap(DunsGuideBook::getId, book -> book));

        Map<String, DunsGuideBook> books = logs.stream().map(patchLog -> {
            Pair<String, DunsGuideBook> target = targetGuideBooks.get(patchLog.getPatchBookId());
            if (target.getValue() == null) {
                target = Pair.of(target.getKey(), dunsGuideBookMap.get(target.getKey()));
            }
            String keyPartition = MatchKeyUtils.evalKeyPartition(patchLog.getInputMatchKey());
            if (cleanupFlagMap.get(patchLog.getPatchBookId())) {
                return cleanupDunsGuideBook(target, keyPartition);
            } else {
                return upsertDunsGuideBook(target, keyPartition, patchDunsMap.get(patchLog.getPatchBookId()));
            }
        }).collect(Collectors.toMap(DunsGuideBook::getId, book -> book, (v1, v2) -> {
            // should be the same instance, just in case
            Preconditions.checkArgument(v1 == v2);
            return v1;
        }));

        // TODO add batching and retry
        dunsGuideBookService.set(dataCloudVersion, new ArrayList<>(books.values()));

        // log basic stats
        long cleanupEntryCount = logs
                .stream()
                .filter(patchLog -> Boolean.TRUE.equals(cleanupFlagMap.get(patchLog.getPatchBookId())))
                .count();
        log.info("Total # of DunsGuideBook entries patched = {}, # of cleanups = {}, # of update = {}",
                books.size(), cleanupEntryCount, books.size() - cleanupEntryCount);
    }

    /*
     * Update or create a DunsGuideBook entry and set targetDuns of the item associated with given key partition to
     * patchDuns.
     *
     * Return the created/updated DunsGuideBook entry for update
     */
    private DunsGuideBook upsertDunsGuideBook(
            @NotNull Pair<String, DunsGuideBook> target, @NotNull String keyPartition, @NotNull String patchDuns) {
        DunsGuideBook dunsGuideBook = target.getValue();
        if (dunsGuideBook.getItems() == null) {
            dunsGuideBook.setItems(new ArrayList<>());
        }

        DunsGuideBook.Item targetItem = getTargetItemByKeyPartition(target, keyPartition);
        if (targetItem != null) {
            targetItem.setPatched(true);
            targetItem.setDuns(patchDuns);
            return dunsGuideBook;
        }

        // no existing item with the same keyPartition, create one
        DunsGuideBook.Item item = new DunsGuideBook.Item();
        item.setDuns(patchDuns);
        item.setKeyPartition(keyPartition);
        item.setPatched(true);
        // new item that does not have book source, create for it
        item.setBookSource(BOOK_SOURCE_PATCHER);
        dunsGuideBook.getItems().add(item);
        return dunsGuideBook;
    }

    /*
     * Remove DunsGuideBook.Item associated with [matchedDUNS, keyPartition] from the given DunsGuideBook entry.
     *
     * The corresponding item must exist in the input DunsGuideBook entry (unless there are multiple patch book entries
     * that want to cleanup the same DunsGuideBook.Item)
     */
    private DunsGuideBook cleanupDunsGuideBook(
            @NotNull Pair<String, DunsGuideBook> target, @NotNull String keyPartition) {
        // should not happen
        Preconditions.checkNotNull(target);
        Preconditions.checkNotNull(target.getValue());

        DunsGuideBook book = target.getValue();
        DunsGuideBook.Item targetItem = getTargetItemByKeyPartition(target, keyPartition);

        // remove the exact object
        if (targetItem != null) {
            book.getItems().removeIf(item -> item == targetItem);
        }

        return book;
    }

    /*
     * no need to patch inactive entries, generate log directly
     */
    private List<PatchLog> generateLogForInactiveBooks(@NotNull List<PatchBook> books) {
        final Date now = new Date();
        final Date start = new Date(Long.MIN_VALUE);
        return books.stream().map(book -> {
            Date effectiveSince = book.getEffectiveSince() == null ? start : book.getEffectiveSince();
            PatchLog log = PatchBookUtils.newPatchLog(book);
            // EOF == false && current end of life => new inactive entry
            log.setStatus(book.isEndOfLife() ? PatchStatus.Inactive : PatchStatus.NewInactive);
            if (effectiveSince.after(now)) {
                // not effective yet
                log.setMessage(String.format(
                        "Entry is not effective yet. EffectiveSince=%s,ExpiredAfter=%s",
                        book.getEffectiveSince(), book.getExpireAfter()));
            } else {
                // already expired
                log.setMessage(String.format(
                        "Entry already expired. EffectiveSince=%s,ExpiredAfter=%s",
                        book.getEffectiveSince(), book.getExpireAfter()));
            }
            return log;

        }).collect(Collectors.toList());
    }

    /*
     * update EOF flags for all required entries (has change in EOF flag)
     */
    private void updateEOF(@NotNull List<PatchBook> inactiveBooks, @NotNull List<PatchBook> activeBooks) {
        List<Long> newActivePatchBookIds = activeBooks
                .stream()
                .filter(PatchBook::isEndOfLife) // originally EOF == true
                .map(PatchBook::getPid)
                .collect(Collectors.toList());
        List<Long> newInactivePatchBookIds = inactiveBooks
                .stream()
                .filter(book -> !book.isEndOfLife()) // originally EOF == false
                .map(PatchBook::getPid)
                .collect(Collectors.toList());
        if (!newActivePatchBookIds.isEmpty()) {
            log.info("Set end of life flag to TRUE for PatchBooks, size = {}", newActivePatchBookIds.size());
            patchBookEntityMgr.setEndOfLife(newActivePatchBookIds, true);
        }
        if (!newInactivePatchBookIds.isEmpty()) {
            log.info("Update end of life flag to FALSE for PatchBooks, size = {}", newActivePatchBookIds.size());
            patchBookEntityMgr.setEndOfLife(newInactivePatchBookIds, false);
        }
    }

    /*
     * update hotfix flag for all required entries (successfully patched), only update if in hotfix mode
     */
    private void updateHotFix(@NotNull PatchMode mode, @NotNull List<PatchLog> patchLogs) {
        if (mode != PatchMode.HotFix) {
            log.info("Not in hotfix mode, skip updating hotfix flags");
            return;
        }
        List<Long> patchedBookIds = patchLogs
                .stream()
                .filter(patchLog -> patchLog.getStatus() == PatchStatus.Patched)
                .map(PatchLog::getPatchBookId)
                .collect(Collectors.toList());
        if (!patchedBookIds.isEmpty()) {
            // reset hotfix flag
            log.info("Set hotfix flag to FALSE for PatchBooks, size = {}", patchedBookIds.size());
            patchBookEntityMgr.setHotFix(patchedBookIds, false);
        }
    }

    /*
     * set effectiveSinceVersion to given version for all active entries that has null in this field
     */
    private void updateEffectiveSinceVersion(
            @NotNull String dataCloudVersion, @NotNull List<PatchBook> activeBooks) {
        List<Long> bookIds = activeBooks
                .stream()
                .filter(book -> book.getEffectiveSince() == null)
                .map(PatchBook::getPid)
                .collect(Collectors.toList());
        if (!bookIds.isEmpty()) {
            log.info("Set effectiveSinceVersion to {} for PatchBooks, size = {}", dataCloudVersion, bookIds.size());
            patchBookEntityMgr.setEffectiveSinceVersion(bookIds, dataCloudVersion);
        }
    }

    /*
     * set ExpireAfterVersion to given version for all inactive entries that has null in this field
     * clear ExpireAfterVersion for all active entries
     */
    private void updateExpireAfterVersion(
            @NotNull String dataCloudVersion,
            @NotNull List<PatchBook> inactiveBooks, @NotNull List<PatchBook> activeBooks) {
        List<Long> activeBookIds = activeBooks.stream().map(PatchBook::getPid).collect(Collectors.toList());
        List<Long> inactiveBookIds = inactiveBooks
                .stream()
                .filter(book -> book.getExpireAfterVersion() == null) // not expire before
                .map(PatchBook::getPid)
                .collect(Collectors.toList());
        if (!activeBookIds.isEmpty()) {
            // cleanup expireAfterVersion for entries that are active
            log.info("Set expireAfterVersion to NULL for PatchBooks, size = {}",
                    dataCloudVersion, activeBookIds.size());
            patchBookEntityMgr.setExpireAfterVersion(activeBookIds, null);
        }
        if (!inactiveBookIds.isEmpty()) {
            log.info("Set expireAfterVersion to {} for PatchBooks, size = {}",
                    dataCloudVersion, inactiveBookIds.size());
            // expire at current version
            patchBookEntityMgr.setExpireAfterVersion(inactiveBookIds, dataCloudVersion);
        }
    }

    /*
     * Generate S3 key with given patch info
     *
     * Format: <PatchBook.Type>_<DataCloudVersion>_<DryRun>_<StartTime>
     * E.g., lookup_2.0.13_normal_true_1540412051045
     */
    private String getPatchLogS3Key(
            @NotNull PatchMode mode, @NotNull PatchBook.Type type, boolean dryRun,
            @NotNull String dataCloudVersion, @NotNull Date startAt) {
        String filename = String.join("_",
                type.name(), dataCloudVersion, mode.name(), String.valueOf(dryRun), String.valueOf(startAt.getTime()));
        return new Path(patchLogDir, filename.toLowerCase()).toS3Key();
    }

    @VisibleForTesting
    void patch(LookupUpdateRequest updateRequest) {
        verifyCanPath(updateRequest);

        OutputRecord outputRecord = match(updateRequest.getMatchKeys());
        verifyNeedToPatch(outputRecord, updateRequest.getLatticeAccountId());

        if (isDomainOnlyRequest(updateRequest)) {
            if (StringUtils.isNotEmpty(outputRecord.getPreMatchDomain())
                    && !publicDomainService.isPublicDomain(outputRecord.getPreMatchDomain())) {
                patchDomainOnly(outputRecord.getPreMatchDomain(), updateRequest.getLatticeAccountId());
                return;
            } else {
                throw new LedpException(LedpCode.LEDP_25035, new String[] { updateRequest.getMatchKeys().getDomain() });
            }
        }

        patchWithNameLocation(updateRequest, outputRecord);
    }

    @VisibleForTesting
    void verifyCanPath(LookupUpdateRequest updateRequest) {
        // must have lattice account id
        if (StringUtils.isEmpty(updateRequest.getLatticeAccountId())) {
            throw new LedpException(LedpCode.LEDP_25030);
        }

        // lattice account id must be valid
        String latticeAccountId = updateRequest.getLatticeAccountId();
        List<LatticeAccount> accountList = accountLookupService
                .batchFetchAccounts(Collections.singletonList(latticeAccountId), currentVersion);
        if (accountList == null || accountList.isEmpty() || accountList.get(0) == null) {
            throw new LedpException(LedpCode.LEDP_25031, new String[] { latticeAccountId });
        }
        LatticeAccount targetAccount = accountList.get(0);
        Object dunsObj = targetAccount.getAttributes().get(MatchConstants.AM_DUNS_FIELD);
        updateRequest.setTargetDuns(dunsObj == null ? null : dunsObj.toString());

        MatchKeyTuple keyTuple = updateRequest.getMatchKeys();

        // must provide non empty match keys
        if (keyTuple == null || (!keyTuple.hasDomain() && !keyTuple.hasLocation() && keyTuple.hasName())) {
            throw new LedpException(LedpCode.LEDP_25032);
        }

        // cannot be domain + location, without name
        if (keyTuple.hasDomain() && keyTuple.hasLocation() && !keyTuple.hasName()) {
            throw new LedpException(LedpCode.LEDP_25033);
        }

    }

    private boolean isDomainOnlyRequest(LookupUpdateRequest updateRequest) {
        MatchKeyTuple keyTuple = updateRequest.getMatchKeys();
        return keyTuple.hasDomain() && !keyTuple.hasName() && !keyTuple.hasLocation();
    }

    private void patchDomainOnly(String domain, String latticeAccountId) {
        AccountLookupEntry lookupEntry = new AccountLookupEntry();
        lookupEntry.setDomain(domain);
        lookupEntry.setLatticeAccountId(latticeAccountId);
        accountLookupService.updateLookupEntry(lookupEntry, currentVersion);
    }

    private void patchWithNameLocation(LookupUpdateRequest updateRequest, OutputRecord outputRecord) {
        String matchedDuns = outputRecord.getMatchedDuns();
        List<String> matchLogs = outputRecord.getMatchLogs();
        List<String> dnbCacheIds = outputRecord.getDnbCacheIds();
        String prematchedDomain = outputRecord.getPreMatchDomain();

        String duns = null;
        if (StringUtils.isNotEmpty(matchedDuns)) {
            String[] tokens = matchedDuns.split(",");
            if (tokens.length == 1) {
                duns = tokens[0];
            }
        }

        if (StringUtils.isEmpty(duns) && dnbCacheIds != null && dnbCacheIds.size() == 1) {
            String dnbCacheId = dnbCacheIds.get(0);
            DnBCache dnBCache = dnBCacheService.getCacheMgr().findByKey(dnbCacheId);
            if (dnBCache == null || dnBCache.getCacheContext() == null
                    || !dnBCache.getCacheContext().containsKey("Duns")
                    || StringUtils.isEmpty((String) dnBCache.getCacheContext().get("Duns"))) {
                if (matchLogs != null) {
                    log.info("Match Logs: \n" + StringUtils.join(matchLogs, "\n"));
                }
                throw new LedpException(LedpCode.LEDP_25036, new String[]{String.valueOf(matchedDuns)});
            } else {
                duns = (String) dnBCache.getCacheContext().get("Duns");
            }
            dnBCache.setPatched(true);
            dnBCacheService.getCacheMgr().create(dnBCache);
            log.info("Updated dnb cache entry with id=" + dnbCacheId + " to patched.");
        }

        if (StringUtils.isEmpty(duns)) {
            if (matchLogs != null) {
                log.info("Match Logs: \n" + StringUtils.join(matchLogs, "\n"));
            }
            throw new LedpException(LedpCode.LEDP_25036, new String[] { String.valueOf(matchedDuns) });
        }

        log.info("Duns to be patched is " + duns);

        AccountLookupEntry lookupEntry = new AccountLookupEntry();
        lookupEntry.setLatticeAccountId(updateRequest.getLatticeAccountId());
        lookupEntry.setDuns(duns);

        AccountLookupRequest request = new AccountLookupRequest(currentVersion);
        request.addLookupPair(null, duns);
        List<String> ids = accountLookupService.batchLookupIds(request);
        if (ids == null || StringUtils.isEmpty(ids.get(0))) {
            log.info("The to be patched duns does not exist in lookup table. Adding it");
            accountLookupService.updateLookupEntry(lookupEntry, currentVersion);
        }

        if (StringUtils.isNotEmpty(prematchedDomain) && !publicDomainService.isPublicDomain(prematchedDomain)) {
            request = new AccountLookupRequest(currentVersion);
            request.addLookupPair(prematchedDomain, duns);
            ids = accountLookupService.batchLookupIds(request);
            if (ids == null || StringUtils.isEmpty(ids.get(0))) {
                log.info("Patching domain + duns lookup.");
                lookupEntry.setDomain(prematchedDomain);
                accountLookupService.updateLookupEntry(lookupEntry, currentVersion);
            }
        } else {
            log.warn("Input domain is ignored. It is either an invalid domain or a public domain.");
        }
    }

    private OutputRecord match(MatchKeyTuple keyTuple) {
        MatchInput matchInput = prepareMatchInput(keyTuple);
        MatchOutput output = realTimeMatchService.match(matchInput);
        return output.getResult().get(0);
    }

    private void verifyNeedToPatch(OutputRecord outputRecord, String latticeAccountId) {
        if (outputRecord != null && StringUtils.isNotEmpty(outputRecord.getMatchedLatticeAccountId())) {
            String matchedId = outputRecord.getMatchedLatticeAccountId();
            String standardizedLatticeAccountId =
                    StringStandardizationUtils.getStandardizedOutputLatticeID(latticeAccountId);

            if (standardizedLatticeAccountId == null) {
                throw new LedpException(LedpCode.LEDP_25031, new String[] { latticeAccountId });
            }

            if (standardizedLatticeAccountId.equals(matchedId)) {
                throw new LedpException(LedpCode.LEDP_25034, new String[] { matchedId });
            }
        }
    }

    private MatchInput prepareMatchInput(MatchKeyTuple keyTuple) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogLevelEnum(Level.DEBUG);
        matchInput.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        List<Object> row = Arrays.asList(keyTuple.getDomain(), keyTuple.getName(), keyTuple.getCity(),
                keyTuple.getState(), keyTuple.getCountry(), keyTuple.getZipcode());
        List<List<Object>> data = Collections.singletonList(row);
        matchInput.setData(data);
        matchInput.setFields(Arrays.asList("Domain", "Name", "City", "State", "Country", "ZipCode"));
        return matchInput;
    }

}
