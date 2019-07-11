package com.latticeengines.datacloud.match.service.impl;

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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.esotericsoftware.minlog.Log;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.util.PatchBookUtils;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.PatchBookValidator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook.Type;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;

@Component("patchBookValidator")
public class PatchBookValidatorImpl implements PatchBookValidator {

    private static final Logger log = LoggerFactory.getLogger(PatchBookValidatorImpl.class);

    private static final long AM_COL_CACHE_TTL_IN_HRS = 1L; // 1 hr ttl, add to property file later if needed
    // TODO remove dummy version after source attr table has versioning
    private static final String DUMMY_SRC_ATTR_DC_VERSION = "2.0.999"; // dummy version for src attr cache

    @Autowired
    @Qualifier("accountMasterColumnEntityMgr")
    private MetadataColumnEntityMgr<AccountMasterColumn> columnEntityMgr;

    @Inject
    private CountryCodeService countryCodeService;

    @Autowired
    @Qualifier("sourceAttributeEntityMgr")
    private SourceAttributeEntityMgr sourceAttributeEntityMgr;

    @Inject
    private DataCloudVersionEntityMgr datacloudVersionEntityMgr;

    private volatile LoadingCache<String, List<AccountMasterColumn>> amColumnCache;
    private volatile LoadingCache<String, List<SourceAttribute>> srcAttrCache;

    @Override
    public Pair<Integer, List<PatchBookValidationError>> validate(
            @NotNull PatchBook.Type type, @NotNull String dataCloudVersion, @NotNull List<PatchBook> books) {
        // Preconditions.checkNotNull(books);
        Preconditions.checkNotNull(type);
        if (books.isEmpty()) {
            return Pair.of(0, Collections.emptyList());
        }

        // only validate entries with specified type and not end of life yet
        Date now = new Date();
        books = books.stream().filter(book -> {
            if (book == null || !type.equals(book.getType())) {
                return false;
            }
            return !PatchBookUtils.isEndOfLife(book, now);
        }).collect(Collectors.toList());
        // standardize field first
        books.forEach(book -> PatchBookUtils.standardize(book, countryCodeService));

        // common validation
        List<PatchBookValidationError> errors = validateCommon(books);

        // type specific validation
        switch (type) {
            case Attribute:
                errors.addAll(validateAttributePatchBook(dataCloudVersion, books));
                break;
            case Domain:
                errors.addAll(validateDomainPatchBook(dataCloudVersion, books));
                break;
            case Lookup:
                errors.addAll(validateLookupPatchBook(dataCloudVersion, books));
                break;
            default:
                String msg = String.format("PatchBook type = %s is not supported for validation", type);
                throw new UnsupportedOperationException(msg);
        }

        return Pair.of(books.size(), errors);
    }

    /*
     * Check if patchKeyItem Key present in AccountMasterColumn and also not
     * among the list of items which cannot be patched then standardize
     */
    @VisibleForTesting
    List<PatchBookValidationError> validatePatchKeyItemAndStandardize(List<PatchBook> books,
            String dataCloudVersion) {
        Set<String> excludePatchItems = new HashSet<>(Arrays.asList(DataCloudConstants.LATTICE_ACCOUNT_ID,
                DataCloudConstants.ATTR_LDC_DOMAIN, DataCloudConstants.ATTR_LDC_DUNS,
                DataCloudConstants.ATTR_LDC_DOMAIN_SOURCE, DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN,
                DataCloudConstants.ATTR_IS_PRIMARY_LOCATION, DataCloudConstants.ATTR_IS_PRIMARY_ACCOUNT,
                DataCloudConstants.ATTR_IS_CTRY_PRIMARY_LOCATION, DataCloudConstants.ATTR_IS_ST_PRIMARY_LOCATION,
                DataCloudConstants.ATTR_IS_ZIP_PRIMARY_LOCATION));
        List<PatchBookValidationError> patchBookValidErrorList = new ArrayList<>();
        List<AccountMasterColumn> amColsList = getAMColumns(dataCloudVersion);
        // Collected AMColumnIds that we need to compare into a set
        Map<String, AccountMasterColumn> amColsMap = new HashMap<>();
        for (AccountMasterColumn a : amColsList) { // mapping columnId to accMasterColumn
            amColsMap.put(a.getAmColumnId(), a);
        }
        Map<String, List<Long>> errNotInAmAndExcluded = new HashMap<>();
        List<Long> attrCleanupKeys = new ArrayList<>();
        // Iterate through input patch Books
        for (PatchBook book : books) {
            List<String> encodedAttrs = new ArrayList<>();
            // Resetting the error message values as null
            List<String> keysNotInAm = new ArrayList<>();
            List<String> keysExcluded = new ArrayList<>();
            // Iterate through the patchItems map to verify if one of AMColumn or from excluded patch list item
            for (Map.Entry<String, Object> patchedItems : book.getPatchItems().entrySet()) {
                String patchAttr = patchedItems.getKey();
                if (!amColsMap.containsKey(patchAttr)) {
                    keysNotInAm.add(patchAttr);
                } else {
                    if (amColsMap.get(patchAttr).getDecodeStrategy() != null) {
                        // Doesn't support patching for encoded attributes
                        encodedAttrs.add(patchAttr);
                    }
                }
                if (excludePatchItems.contains(patchAttr)) {
                    keysExcluded.add(patchAttr);
                }
            }
            if (book.getType().equals(Type.Attribute) && book.isCleanup()) {
                attrCleanupKeys.add(book.getPid());
            }
            if (encodedAttrs.size() > 0) {
                PatchBookValidationError error = new PatchBookValidationError();
                error.setMessage(ENCODED_ATTRS_NOT_SUPPORTED + encodedAttrs.toString());
                error.setPatchBookIds(Arrays.asList(book.getPid()));
                patchBookValidErrorList.add(error);
            }
            if (!keysNotInAm.isEmpty()) {
                errNotInAmAndExcluded.putIfAbsent(PATCH_ITEM_NOT_IN_AM + keysNotInAm, new ArrayList<>());
                errNotInAmAndExcluded.get(PATCH_ITEM_NOT_IN_AM + keysNotInAm).add(book.getPid());
            }
            if (!keysExcluded.isEmpty()) {
                errNotInAmAndExcluded.putIfAbsent(EXCLUDED_PATCH_ITEM + keysExcluded, new ArrayList<>());
                errNotInAmAndExcluded.get(EXCLUDED_PATCH_ITEM + keysExcluded).add(book.getPid());
            }
        }
        // Return ErrorList
        for (Map.Entry<String, List<Long>> itemNotInAmOrExcluded : errNotInAmAndExcluded.entrySet()) {
            PatchBookValidationError error = new PatchBookValidationError();
            error.setMessage(itemNotInAmOrExcluded.getKey());
            error.setPatchBookIds(itemNotInAmOrExcluded.getValue());
            patchBookValidErrorList.add(error);
        }
        if (attrCleanupKeys.size() > 0) {
            PatchBookValidationError error = new PatchBookValidationError();
            error.setMessage(ERR_ATTRI_CLEANUP);
            error.setPatchBookIds(attrCleanupKeys);
            patchBookValidErrorList.add(error);
        }
        return patchBookValidErrorList;
    }

    /*
     * Helper function to check conflict in patchItems
     */
    private boolean hasConflict(Map<String, Object> patchItems1, Map<String, Object> patchItems2) {
        for (Map.Entry<String, Object> patchItem : patchItems2.entrySet()) {
            if (patchItems1 != null && patchItems1.containsKey(patchItem.getKey())) {
                Object patchItemValue = patchItems1.get(patchItem.getKey());
                if (patchItemValue != null && !patchItemValue.equals(patchItem.getValue())) { // ensuring same patchItem with different value for same key
                    return true; // hasConflict
                }
            }
        }
        return false;
    }

    /*
     * Check if any conflict in patch attributes within one match key and across
     * all match keys
     */
    @VisibleForTesting
    List<PatchBookValidationError> validateConflictInPatchItems(List<PatchBook> books,
            String dataCloudVersion) {
        // seperator
        String seperator = "_AND_";
        /* Populate all the maps */
        // matchKey = Domain -> PatchBook
        Map<String, PatchBook> domainMap = new HashMap<>();
        // matchKey = DUNS -> PatchBook
        Map<String, PatchBook> dunsMap = new HashMap<>();
        // matchKey = Domain+DUNS -> PatchBook
        Map<String, PatchBook> domainDunsMap = new HashMap<>();
        // Iterate through input patch Books and populate perspective maps
        List<PatchBookValidationError> patchBookValidErrorList = new ArrayList<>();
        for (PatchBook book : books) {
            if(!StringUtils.isEmpty(book.getDomain()) && StringUtils.isEmpty(book.getDuns())) { // domain only match key
                domainMap.put(book.getDomain(), book);
            }
            if(StringUtils.isEmpty(book.getDomain()) && !StringUtils.isEmpty(book.getDuns())) { // DUNS only match key
                dunsMap.put(book.getDuns(), book);
            }
            if(!StringUtils.isEmpty(book.getDomain()) && !StringUtils.isEmpty(book.getDuns())) { // domain+DUNS match key
                domainDunsMap.put(book.getDomain() + seperator + book.getDuns(), book);
            }
        }
        List<Long> conflictPids = new ArrayList<>();
        // Iterate domainDuns Map and check if individual domain/duns present in domainMap or dunsMap
        for (String item : domainDunsMap.keySet()) {
            String domain = item.split(seperator)[0];
            String duns = item.split(seperator)[1];
            PatchBook domainDunsPatchBook = domainDunsMap.get(domain + seperator + duns);
            Long pid = domainDunsPatchBook.getPid();
            if (domainMap.containsKey(domain)
                    && hasConflict(domainMap.get(domain).getPatchItems(), domainDunsPatchBook.getPatchItems())) {
                conflictPids.add(domainMap.get(domain).getPid());
                if (!conflictPids.contains(pid)) {
                    conflictPids.add(pid);
                }
            }
            if (dunsMap.containsKey(duns)
                    && hasConflict(dunsMap.get(duns).getPatchItems(), domainDunsPatchBook.getPatchItems())) {
                conflictPids.add(dunsMap.get(duns).getPid());
                if (!conflictPids.contains(pid)) {
                    conflictPids.add(pid);
                }
            }
        }
        if (!conflictPids.isEmpty()) {
            PatchBookValidationError error = new PatchBookValidationError();
            error.setMessage(CONFLICT_IN_PATCH_ITEM);
            error.setPatchBookIds(conflictPids);
            patchBookValidErrorList.add(error);
        }
        return patchBookValidErrorList;
    }

    private List<PatchBookValidationError> validateCommon(@NotNull List<PatchBook> books) {
        List<PatchBookValidationError> errors = new ArrayList<>();
        errors.addAll(PatchBookUtils.validateMatchKeySupport(books));
        errors.addAll(PatchBookUtils.validatePatchedItems(books));
        errors.addAll(PatchBookUtils.validateEffectiveDateRange(books));
        return errors;
    }

    private List<PatchBookValidationError> validateAttributePatchBook(
            @NotNull String dataCloudVersion, @NotNull List<PatchBook> books) {
        List<PatchBookValidationError> errors = new ArrayList<>();
        errors.addAll(PatchBookUtils.validateDuplicateMatchKey(books));
        errors.addAll(validatePatchKeyItemAndStandardize(books, dataCloudVersion));
        errors.addAll(validateSourceAttribute(books, dataCloudVersion));
        errors.addAll(validateConflictInPatchItems(books, dataCloudVersion));
        return errors;
    }

    List<PatchBookValidationError> domainPatchValidate(@NotNull List<PatchBook> books) {
        Map<Pair<String, String>, List<Long>> patchBooksWithSameDomDuns = new HashMap<>();
        List<PatchBookValidationError> errorList = new ArrayList<>();
        List<Long> errPatchItemsPids = new ArrayList<>();
        for (PatchBook book : books) {
            Pair<String, String> dunsKeyAndDomPatchItem = Pair.of(book.getDuns(),
                    PatchBookUtils.getPatchDomain(book));
            if (book.getPatchItems().containsKey(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Domain))
                    && book.getPatchItems().size() == 1) {
                // match key = duns and patchedItem domain
                // check if match key = duns and patchedItem = domain, this combination is unique
                patchBooksWithSameDomDuns.putIfAbsent(dunsKeyAndDomPatchItem, new ArrayList<>());
                patchBooksWithSameDomDuns.get(dunsKeyAndDomPatchItem).add(book.getPid());
            } else {
                errPatchItemsPids.add(book.getPid());
            }
        }
        for (Pair<String, String> domDuns : patchBooksWithSameDomDuns.keySet()) {
            List<Long> pids = patchBooksWithSameDomDuns.get(domDuns);
            if (pids.size() > 1) {
                PatchBookValidationError error = new PatchBookValidationError();
                error.setMessage(DUPLI_MATCH_KEY_AND_PATCH_ITEM_COMBO + "DUNS = "
                        + domDuns.getKey() + " PatchDomain = " + domDuns.getValue());
                error.setPatchBookIds(pids);
                errorList.add(error);
            }
        }
        if (errPatchItemsPids.size() > 0) {
            PatchBookValidationError error = new PatchBookValidationError();
            error.setMessage(ERR_IN_PATCH_ITEMS);
            System.out.println("error msg : " + error.getMessage());
            error.setPatchBookIds(errPatchItemsPids);
            errorList.add(error);
        }
        return errorList;
    }

    /*
     * Enhancement of Attribute Patch Validation API based on domain / duns
     * based sources
     */
    @SuppressWarnings("deprecation")
    List<PatchBookValidationError> validateSourceAttribute(@NotNull List<PatchBook> books,
            @NotNull String dataCloudVersion) {
        /* extract attributes based on domain and duns based source */
        List<SourceAttribute> sourceAttributes = getSourceAttributes(dataCloudVersion);
        Set<String> attrNamesForDomBasedSources = new HashSet<>();
        Set<String> attrNamesForDunsBasedSources = new HashSet<>();
        for (SourceAttribute srcAttr : sourceAttributes) {
            String argument = srcAttr.getArguments();
            String attribute = srcAttr.getAttribute();
            ObjectMapper mapper = new ObjectMapper();
            String source = null;
            try {
                source = mapper.readTree(argument).getPath("Source").asText();
            } catch (Exception e) {
                Log.error("Error in reading Json.");
                continue;
            }
            // populate set of attribute names for domain based sources
            if (DataCloudConstants.DOMAIN_BASED_SOURCES.contains(source)) {
                attrNamesForDomBasedSources.add(attribute);
            }
            // populate set of attribute names for duns based sources
            if (DataCloudConstants.DUNS_BASED_SOURCES.contains(source)) {
                attrNamesForDunsBasedSources.add(attribute);
            }
        }
        List<PatchBookValidationError> patchBookValidErrorList = new ArrayList<>();
        // Iterate through the patchItems to check patch item attribute is from
        // domain based source or duns based source
        for (PatchBook book : books) {
            List<String> domBasedSrcAbsentAttrs = new ArrayList<>();
            List<String> dunsBasedSrcAbsentAttrs = new ArrayList<>();
            Map<String, Object> patchItems = book.getPatchItems();
            for (Map.Entry<String, Object> patchedItem : patchItems.entrySet()) {
                String patchAttr = patchedItem.getKey();
                if (StringUtils.isBlank(book.getDomain())
                        && attrNamesForDomBasedSources.contains(patchAttr)) {
                    // Error : No domain match key and wants to patch domain source attr
                    domBasedSrcAbsentAttrs.add(patchAttr);
                }
                if (StringUtils.isBlank(book.getDuns())
                        && attrNamesForDunsBasedSources.contains(patchAttr)) {
                    // Error : No duns match key and wants to patch duns source attr
                    dunsBasedSrcAbsentAttrs.add(patchAttr);
                }
            }
            PatchBookValidationError error = reportErrorsForAttrPatchValidator(
                    domBasedSrcAbsentAttrs, dunsBasedSrcAbsentAttrs, book.getPid());
            if (error.getMessage() != null) {
                patchBookValidErrorList.add(error);
            }
        }
        return patchBookValidErrorList;
    }

    private PatchBookValidationError reportErrorsForAttrPatchValidator(
            List<String> domBasedSrcAbsentAttrs,
            List<String> dunsBasedSrcAbsentAttrs, Long pid) {
        List<Long> pids = new ArrayList<>();
        PatchBookValidationError error = new PatchBookValidationError();
        if (domBasedSrcAbsentAttrs.size() > 0) {
            error.setMessage(ATTRI_PATCH_DOM_BASED_SRC_ERR + domBasedSrcAbsentAttrs.toString());
            pids.add(pid);
            domBasedSrcAbsentAttrs.clear();
        }
        if (dunsBasedSrcAbsentAttrs.size() > 0) {
            error.setMessage(ATTRI_PATCH_DUNS_BASED_SRC_ERR + dunsBasedSrcAbsentAttrs.toString());
            pids.add(pid);
            dunsBasedSrcAbsentAttrs.clear();
        }
        if (pids.size() > 0) {
            error.setPatchBookIds(pids);
        }
        return error;
    }

    private List<PatchBookValidationError> validateLookupPatchBook(
            @NotNull String dataCloudVersion, @NotNull List<PatchBook> books) {
        // match key combination must be unique
        List<PatchBookValidationError> errorList = PatchBookUtils.validateDuplicateMatchKey(books);

        Map<String, List<Long>> errorMap = books
                .stream()
                // only check supported match key
                .filter(PatchBookUtils::isMatchKeySupported)
                .flatMap(book -> {
                    // both DunsGuideBook & AMLookup patch need DUNS
                    Long pid = book.getPid();
                    // Pair.of(ErrorMessage, PatchBookId)
                    List<Pair<String, Long>> errors = new ArrayList<>();
                    if (!isPatchDunsValid(book)) {
                        errors.add(Pair.of(String.format(
                                "Invalid DUNS = %s in patchItems", PatchBookUtils.getPatchDuns(book)), pid));
                    }
                    if (PatchBookUtils.shouldPatchAMLookupTable(book)) {
                        // AM lookup patch, need to have Domain + DUNS
                        if (!isPatchDomainValid(book)) {
                            errors.add(Pair.of(String.format(
                                    "Invalid Domain = %s in patchItems", PatchBookUtils.getPatchDomain(book)), pid));
                        }
                        if (book.getPatchItems().size() != 2) {
                            errors.add(Pair.of("Should only have Domain + DUNS in patchItems", pid));
                        }
                    } else if (book.getPatchItems().size() != 1) {
                        // DunsGuideBook patch, need to have DUNS
                        errors.add(Pair.of("Should only have DUNS in patchItems", pid));
                    }
                    return errors.stream();
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Pair::getKey, pair -> Lists.newArrayList(pair.getValue()), ListUtils::union));

        // generate errors
        errorList.addAll(errorMap.entrySet().stream().map(entry -> {
            PatchBookValidationError error = new PatchBookValidationError();
            error.setPatchBookIds(entry.getValue());
            error.setMessage(entry.getKey());
            Collections.sort(error.getPatchBookIds());
            return error;
        }).collect(Collectors.toList()));
        return errorList;
    }

    private List<PatchBookValidationError> validateDomainPatchBook(
            @NotNull String dataCloudVersion, @NotNull List<PatchBook> books) {
        List<PatchBookValidationError> errors = new ArrayList<>();
        errors.addAll(domainPatchValidate(books));
        return errors;
    }

    /*
     * wrappers around in-memory cache
     */
    private List<AccountMasterColumn> getAMColumns(@NotNull String dataCloudVersion) {
        initAmColumnCache();
        return amColumnCache.get(dataCloudVersion);
    }

    private List<SourceAttribute> getSourceAttributes(@NotNull String dataCloudVersion) {
        initSourceAttrCache();
        return srcAttrCache.get(DUMMY_SRC_ATTR_DC_VERSION);
    }

    private void initAmColumnCache() {
        if (amColumnCache != null) {
            return;
        }

        synchronized (this) {
            if (amColumnCache == null) {
                log.info("Instantiating AM column cache");
                amColumnCache = Caffeine.newBuilder() //
                        .expireAfterAccess(AM_COL_CACHE_TTL_IN_HRS, TimeUnit.HOURS) //
                        .build((dataCloudVersion) -> {
                            log.info("Loading AM columns for datacloud version {}", dataCloudVersion);
                            List<AccountMasterColumn> amCols = columnEntityMgr.findAll(dataCloudVersion) //
                                    .sequential().collectList().block();
                            // Temporary solution to solve issue:
                            // When we build new AccountMaster with PatchBook,
                            // AccountMasterColumn table in production doesn't
                            // have new DataCloud version yet, so fall back to
                            // use current approved version -- For now, PM is
                            // not likely to patch an attribute which is not
                            // approved in current AccountMaster in production
                            // TODO: Final solution could be create a new
                            // AccountMasterColumn staging table which could be
                            // added with new DataCloud version during AM
                            // rebuild process without impacting application in
                            // production
                            if (CollectionUtils.isEmpty(amCols)) {
                                String currentVersion = datacloudVersionEntityMgr.currentApprovedVersionAsString();
                                log.info(
                                        "DataCloud version {} doesn't exist in AccountMasterColumn table in production, fall back to load AM columns for version {}",
                                        dataCloudVersion, currentVersion);
                                amCols = columnEntityMgr.findAll(currentVersion) //
                                        .sequential().collectList().block();
                            }
                            return amCols;
                        });
            }
        }
    }

    private void initSourceAttrCache() {
        if (srcAttrCache != null) {
            return;
        }

        synchronized (this) {
            if (srcAttrCache == null) {
                log.info("Instantiating source attribute cache");
                srcAttrCache = Caffeine.newBuilder() //
                        .expireAfterAccess(AM_COL_CACHE_TTL_IN_HRS, TimeUnit.HOURS) //
                        .build((dataCloudVersion) -> {
                            log.info("Loading source attributes for datacloud version {}", dataCloudVersion);
                            // TODO use datacloud version after the table supports it
                            return sourceAttributeEntityMgr.getAttributes("AccountMaster", "MapStage", "mapAttribute",
                                    null, false);
                        });
            }
        }
    }

    /*
     * NOTE: DUNS should already be standardized
     */
    private boolean isPatchDunsValid(@NotNull PatchBook book) {
        return PatchBookUtils.getPatchDuns(book) != null;
    }

    /*
     * NOTE: Domain should already be standardized
     */
    private boolean isPatchDomainValid(@NotNull PatchBook book) {
        return PatchBookUtils.getPatchDomain(book) != null;
    }

}
