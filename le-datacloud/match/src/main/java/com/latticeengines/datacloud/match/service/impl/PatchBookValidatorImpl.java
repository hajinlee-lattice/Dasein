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
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.util.PatchBookUtils;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.PatchBookValidator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;

import reactor.core.publisher.ParallelFlux;

@Component("patchBookValidator")
public class PatchBookValidatorImpl implements PatchBookValidator {

    @Autowired
    @Qualifier("accountMasterColumnEntityMgr")
    private MetadataColumnEntityMgr<AccountMasterColumn> columnEntityMgr;

    @Inject
    private CountryCodeService countryCodeService;

    @Autowired
    @Qualifier("sourceAttributeEntityMgr")
    private SourceAttributeEntityMgr sourceAttributeEntityMgr;

    @Override
    public Pair<Integer, List<PatchBookValidationError>> validate(
            @NotNull PatchBook.Type type, @NotNull String dataCloudVersion, @NotNull List<PatchBook> books) {
        Preconditions.checkNotNull(books);
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
        ParallelFlux<AccountMasterColumn> amCols = columnEntityMgr
                .findAll(dataCloudVersion);
        // Getting List from ParallelFlux
        List<AccountMasterColumn> amColsList = amCols.sequential().collectList().block();
        // Collected AMColumnIds that we need to compare into a set
        Set<String> amColumnIds = new HashSet<>();
        for(AccountMasterColumn a : amColsList){
            amColumnIds.add(a.getAmColumnId());
        }
        Map<String, List<Long>> errNotInAmAndExcluded = new HashMap<>();
        // Iterate through input patch Books
        for (PatchBook book : books) {
            // Resetting the error message values as null
            List<String> keysNotInAm = new ArrayList<>();
            List<String> keysExcluded = new ArrayList<>();
            // Iterate through the patchItems map to verify if one of AMColumn or from excluded patch list item
            for (Map.Entry<String, Object> patchedItems : book.getPatchItems().entrySet()) {
                if (!amColumnIds.contains(patchedItems.getKey())) {
                    keysNotInAm.add(patchedItems.getKey());
                }
                if (excludePatchItems.contains(patchedItems.getKey())) {
                    keysExcluded.add(patchedItems.getKey());
                }
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
        errors.addAll(enhancementOfAttributePatchApi(books, dataCloudVersion));
        return errors;
    }

    /*
     * Enhancement of Attribute Patch Validation API based on domain / duns
     * based sources
     */
    List<PatchBookValidationError> enhancementOfAttributePatchApi(@NotNull List<PatchBook> books, @NotNull String dataCloudVersion) {
        /* HardCoding domain and duns based sources */
        Set<String> domainBasedSources = new HashSet<>();
        Set<String> dunsBasedSources = new HashSet<>();
        String domBasedSources[] = { "AlexaMostRecent", "Bombora30DayAgg", "BomboraSurgePivoted", "BuiltWithPivoted",
                "BuiltWithTechIndicators", "FeaturePivoted", "HGDataPivoted", "HGDataTechIndicators", "HPANewPivoted",
                "OrbIntelligenceMostRecent", "SemrushMostRecent" };
        domainBasedSources.addAll(Arrays.asList(domBasedSources));
        dunsBasedSources.add("DnBCacheSeed");
        /* extract attributes based on domain and duns based source */
        List<SourceAttribute> sourceAttributes = sourceAttributeEntityMgr.getAttributes("AccountMaster", "MapStage",
                "mapAttribute", null, false);
        Set<String> attrNamesForDomBasedSources = new HashSet<>();
        Set<String> attrNamesForDunsBasedSources = new HashSet<>();
        for (SourceAttribute srcAttr : sourceAttributes) {
            String argument = srcAttr.getArguments();
            String attribute = srcAttr.getAttribute();
            String[] argArray = argument.split(",");
            String source = argArray[argArray.length - 1].split(":")[1];
            // populate set of attribute names for domain based sources
            if (domainBasedSources.contains(source)) {
                attrNamesForDomBasedSources.add(attribute);
            }
            // populate set of attribute names for duns based sources
            if (dunsBasedSources.contains(source)) {
                attrNamesForDunsBasedSources.add(attribute);
            }
        }
        List<PatchBookValidationError> patchBookValidErrorList = new ArrayList<>();
        // Iterate through the patchItems to check patch item attribute is from domain based source or duns based source
        for(PatchBook book : books) {
            boolean domMatchKeyPresent = false;
            boolean dunsMatchKeyPresent = false;
            boolean domSrcErrPresent = false;
            boolean dunsSrcErrPresent = false;
            boolean encoded = false;
            List<String> domBasedSrcAbsentAttrs = new ArrayList<>();
            List<String> dunsBasedSrcAbsentAttrs = new ArrayList<>();
            List<String> encodedAttrs = new ArrayList<>();
            Map<String, Object> patchItems = book.getPatchItems();
            // domain Match Key Present
            if (!StringUtils.isEmpty(book.getDomain())) {
                domMatchKeyPresent = true;
            }
            // duns Match Key Present
            if (!StringUtils.isEmpty(book.getDuns())) {
                dunsMatchKeyPresent = true;
            }
            for (Map.Entry<String, Object> patchedItem : patchItems.entrySet()) {
                String patchAttr = patchedItem.getKey();
                if (domMatchKeyPresent) {
                    if (!attrNamesForDomBasedSources.contains(patchAttr)) {
                        // Error : since domain match key is present and patch
                        // attrName should be one of the attr names for domain
                        // based sources
                        domSrcErrPresent = true;
                        domBasedSrcAbsentAttrs.add(patchAttr);
                    }
                }
                if (dunsMatchKeyPresent) {
                    if (!attrNamesForDunsBasedSources.contains(patchAttr)) {
                        // Error : since duns match key is present and patch
                        // attrName should be one of the attr names for duns
                        // based sources
                        dunsSrcErrPresent = true;
                        dunsBasedSrcAbsentAttrs.add(patchAttr);
                    }
                }
                // Doesn't support patching for encoded attributes
                AccountMasterColumn amCol = columnEntityMgr.findById(patchAttr, dataCloudVersion);
                if (amCol.getDecodeStrategy() != null) {
                    encoded = true;
                    encodedAttrs.add(patchAttr);
                }
            }
            PatchBookValidationError error = reportErrorsForAttrPatchValidator(domSrcErrPresent,
                    dunsMatchKeyPresent,
                    dunsSrcErrPresent, domMatchKeyPresent, domBasedSrcAbsentAttrs,
                    dunsBasedSrcAbsentAttrs, encodedAttrs, book.getPid(), encoded);
            if (error.getMessage() != null) {
                patchBookValidErrorList.add(error);
            }
        }
        return patchBookValidErrorList;
    }

    private PatchBookValidationError reportErrorsForAttrPatchValidator(
            boolean domSrcErrPresent, boolean dunsMatchKeyPresent, boolean dunsSrcErrPresent,
            boolean domMatchKeyPresent, List<String> domBasedSrcAbsentAttrs,
            List<String> dunsBasedSrcAbsentAttrs, List<String> encodedAttrs, Long pid,
            boolean encoded) {
        List<Long> pids = new ArrayList<>();
        PatchBookValidationError error = new PatchBookValidationError();
        if (domSrcErrPresent && !dunsMatchKeyPresent) {
            error.setMessage(
                    ATTRI_PATCH_DOM_DUNS_BASED_SRC_ERR + domBasedSrcAbsentAttrs.toString()); 
            domSrcErrPresent = false;
            pids.add(pid);
        }
        if (dunsSrcErrPresent && !domMatchKeyPresent) {
            error.setMessage(
                    ATTRI_PATCH_DOM_DUNS_BASED_SRC_ERR + dunsBasedSrcAbsentAttrs.toString());
            dunsSrcErrPresent = false;
            pids.add(pid);
        }
        if (encoded && !domSrcErrPresent && !dunsSrcErrPresent) {
            error.setMessage(ENCODED_ATTRS_NOT_SUPPORTED + encodedAttrs.toString());
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
        // TODO implement
        return Collections.emptyList();
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
