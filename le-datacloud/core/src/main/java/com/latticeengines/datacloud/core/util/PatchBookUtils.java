package com.latticeengines.datacloud.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.common.exposed.util.LocationUtils;
import com.latticeengines.common.exposed.util.NameStringStandardizationUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchLog;

/**
 * Shared utilities for {@link PatchBook}
 */
public final class PatchBookUtils {

    protected PatchBookUtils() {
        throw new UnsupportedOperationException();
    }

    public static final String DUPLICATE_MATCH_KEY_ERROR = "Duplicate match key combination found : ";
    public static final String INVALID_EFFECTIVE_DATE_RANGE_ERROR =
            "ExpireAfter date should not be earlier than EffectiveSince date";
    public static final String UNSUPPORTED_MATCH_KEY_ERROR = "Unsupported match key combination found. We do not support following combination : ";
    public static final String INVALID_PATCH_ITEMS = "Invalid Patch Items : Empty Patch Item Object Found.";
    public static final String MIN_PID = "MIN";
    public static final String MAX_PID = "MAX";

    // Key: patch book type, Value: set of supported match key tuple (in serialized format)
    private static final Map<PatchBook.Type, Set<String>> SUPPORTED_MATCH_KEY_MAP = new HashMap<>();

    /* lookup patch */

    // serialized match keys that requires patching AM lookup table
    private static final Set<String> AM_LOOKUP_PATCH_MATCH_KEYS = new HashSet<>();
    // serialized match keys that requires patching DunsGuideBook table
    private static final Set<String> DUNS_GUIDE_BOOK_LOOKUP_PATCH_MATCH_KEYS = new HashSet<>();

    static {
        MatchKeyTuple domainDunsTuple = new MatchKeyTuple.Builder() //
                .withDomain(MatchKey.Domain.name()) //
                .withDuns(MatchKey.DUNS.name()) //
                .build(); //
        MatchKeyTuple domainTuple = new MatchKeyTuple.Builder() //
                .withDomain(MatchKey.Domain.name()) //
                .build(); //
        MatchKeyTuple dunsTuple = new MatchKeyTuple.Builder() //
                .withDuns(MatchKey.DUNS.name()) //
                .build(); //
        SUPPORTED_MATCH_KEY_MAP.put(PatchBook.Type.Attribute,
                new HashSet<>(Arrays.asList(domainTuple.buildIdForKey(), dunsTuple.buildIdForKey(),
                        domainDunsTuple.buildIdForKey())));
        MatchKeyTuple domainPatcherType = new MatchKeyTuple.Builder() //
                .withDuns(MatchKey.DUNS.name()) //
                .build(); //
        SUPPORTED_MATCH_KEY_MAP.put(PatchBook.Type.Domain, Collections.singleton(domainPatcherType.buildIdForKey()));

        /* init match key constants for lookup patch */

        // am lookup
        AM_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withDomain(MatchKey.Domain.name()) //
                .build().buildIdForKey()); // Domain
        AM_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withDuns(MatchKey.DUNS.name()) //
                .build().buildIdForKey()); // DUNS
        AM_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withDomain(MatchKey.Domain.name()) //
                .withCountry(MatchKey.Country.name()) //
                .build().buildIdForKey()); // Domain, Country
        AM_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withDomain(MatchKey.Domain.name()) //
                .withCountry(MatchKey.Country.name()) //
                .withState(MatchKey.State.name()) //
                .build().buildIdForKey()); // Domain, Country, State
        AM_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withDomain(MatchKey.Domain.name()) //
                .withCountry(MatchKey.Country.name()) //
                .withZipcode(MatchKey.Zipcode.name()) //
                .build().buildIdForKey()); // Domain, Country, Zipcode

        /* duns guide book */
        DUNS_GUIDE_BOOK_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withName(MatchKey.Name.name()) //
                .build().buildIdForKey()); // Name
        DUNS_GUIDE_BOOK_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withName(MatchKey.Name.name()) //
                .withCountry(MatchKey.Country.name()) //
                .build().buildIdForKey()); // Name, Country
        DUNS_GUIDE_BOOK_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withName(MatchKey.Name.name()) //
                .withCountry(MatchKey.Country.name()) //
                .withState(MatchKey.State.name()) //
                .build().buildIdForKey()); // Name, Country, State
        DUNS_GUIDE_BOOK_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withName(MatchKey.Name.name()) //
                .withCountry(MatchKey.Country.name()) //
                .withCity(MatchKey.City.name()) //
                .build().buildIdForKey()); // Name, Country, City
        DUNS_GUIDE_BOOK_LOOKUP_PATCH_MATCH_KEYS.add(new MatchKeyTuple.Builder() //
                .withName(MatchKey.Name.name()) //
                .withCountry(MatchKey.Country.name()) //
                .withState(MatchKey.State.name()) //
                .withCity(MatchKey.City.name()) //
                .build().buildIdForKey()); // Name, Country, State, City

        // all supported match keys for lookup patch
        SUPPORTED_MATCH_KEY_MAP.put(PatchBook.Type.Lookup,
                Sets.union(AM_LOOKUP_PATCH_MATCH_KEYS, DUNS_GUIDE_BOOK_LOOKUP_PATCH_MATCH_KEYS));
    }

    /**
     * Standardize all input match key fields of target patch book
     *
     * @param book target patch book, should not be {@literal null}
     * @param countryCodeService service to standardize country string, should not be {@literal null}
     */
    public static void standardize(@NotNull PatchBook book, @NotNull CountryCodeService countryCodeService) {
        book.setDomain(DomainUtils.parseDomain(book.getDomain()));
        book.setDuns(StringStandardizationUtils.getStandardDuns(book.getDuns()));
        book.setName(NameStringStandardizationUtils.getStandardString(book.getName()));
        book.setCity(NameStringStandardizationUtils.getStandardString(book.getCity()));
        String cleanedCountry = countryCodeService.getStandardCountry(book.getCountry());
        book.setCountry(cleanedCountry);
        book.setState(LocationUtils.getStandardState(cleanedCountry, book.getState()));
        book.setZipcode(StringStandardizationUtils.getStandardString(book.getZipcode()));
        Map<String, Object> patchedItems = book.getPatchItems();
        String standardizedPatchDomain = DomainUtils.parseDomain(getPatchDomain(book));
        String standardizedPatchDuns = StringStandardizationUtils
                .getStandardDuns(getPatchDuns(book));
        if (patchedItems.containsKey(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Domain))) {
            patchedItems.replace(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Domain), standardizedPatchDomain);
        }
        if (patchedItems.containsKey(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS))) {
            patchedItems.replace(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS), standardizedPatchDuns);
        }
    }

    /**
     * Generate a {@link MatchKeyTuple} that contains values of all match key fields in the given patch book
     *
     * @param book target patch book
     * @return generated tuple
     */
    public static MatchKeyTuple getMatchKeyValues(@NotNull PatchBook book) {
        Preconditions.checkNotNull(book);
        MatchKeyTuple tuple = new MatchKeyTuple();
        if (StringUtils.isNotBlank(book.getDomain())) {
            tuple.setDomain(book.getDomain());
        }
        if (StringUtils.isNotBlank(book.getDuns())) {
            tuple.setDuns(book.getDuns());
        }
        if (StringUtils.isNotBlank(book.getName())) {
            tuple.setName(book.getName());
        }
        if (StringUtils.isNotBlank(book.getCountry())) {
            tuple.setCountry(book.getCountry());
            // for key partition evaluation, not required in patcher
            tuple.setCountryCode(book.getCountry());
        }
        if (StringUtils.isNotBlank(book.getState())) {
            tuple.setState(book.getState());
        }
        if (StringUtils.isNotBlank(book.getCity())) {
            tuple.setCity(book.getCity());
        }
        if (StringUtils.isNotBlank(book.getZipcode())) {
            tuple.setZipcode(book.getZipcode());
        }

        return tuple;
    }

    /**
     * Validate whether there are duplicate match key combination (same combination of match keys AND with the same
     * values) in the given list of patch books.
     *
     * NOTE no filtering will be performed in this method. it is suggested that all input {@link PatchBook} have the
     * same {@link PatchBook.Type}
     *
     * @param patchBooks list of patch books, should not be {@literal null}
     * @return a list of validation errors where each error represents a group of patch books with duplicated match
     * keys
     */
    public static List<PatchBookValidationError> validateDuplicateMatchKey(@NotNull List<PatchBook> patchBooks) {
        Map<String, List<Long>> matchKeyToPIDs = new HashMap<>();
        for (PatchBook entry : patchBooks) {
            MatchKeyTuple tuple = getMatchKeyValues(entry);
            matchKeyToPIDs.putIfAbsent(tuple.buildIdForValue(), new ArrayList<>());
            matchKeyToPIDs.get(tuple.buildIdForValue()).add(entry.getPid());
        }

        return matchKeyToPIDs.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1) // filter by key
                .map(temp -> {
                    PatchBookValidationError error = new PatchBookValidationError();
                    error.setMessage(DUPLICATE_MATCH_KEY_ERROR + temp.getKey());
                    error.setPatchBookIds(temp.getValue());
                    return error;
                }).collect(Collectors.toList());
    }

    /**
     * Validate whether there are patch books that contain unsupported match key combination
     *
     * NOTE no filtering will be performed in this method. it is suggested that all input {@link PatchBook} have the
     * same {@link PatchBook.Type}
     *
     * @param patchBooks list of patch books, should not be {@literal null}
     * @return a list of validation errors to show which patch books have unsupported match keys
     */
    public static List<PatchBookValidationError> validateMatchKeySupport(@NotNull List<PatchBook> patchBooks) {
        Map<String, List<Long>> errorMap = patchBooks.stream() //
                .filter(book -> !isMatchKeySupported(book)) //
                .map(book -> { //
                    String keyId = getMatchKeyValues(book).buildIdForKey(); //
                    return Pair.of(UNSUPPORTED_MATCH_KEY_ERROR + keyId, Lists.newArrayList(book.getPid()));
                }) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, ListUtils::union)); //
        return errorMap.entrySet() //
                .stream() //
                .map(entry -> { //
                    PatchBookValidationError error = new PatchBookValidationError(); //
                    error.setMessage(entry.getKey()); //
                    error.setPatchBookIds(entry.getValue()); //
                    return error; //
                }).collect(Collectors.toList());
    }

    /**
     * Validate whether {@link PatchBook#getEffectiveSince()} and {@link PatchBook#getExpireAfter()} is set correctly.
     * {@literal null} books will be skipped.
     *
     * NOTE no other filtering will be performed in this method. it is suggested that all input {@link PatchBook} have
     * the same {@link PatchBook.Type}
     *
     * @param patchBooks list of {@link PatchBook} to validate, should not be {@literal null}
     * @return a list of validation errors
     */
    public static List<PatchBookValidationError> validateEffectiveDateRange(@NotNull List<PatchBook> patchBooks) {
        Preconditions.checkNotNull(patchBooks);
        List<Long> invalidRangePids = patchBooks
                .stream()
                .filter(book -> book != null && book.getEffectiveSince() != null && book.getExpireAfter() != null)
                // expire time should not be before effective time
                .filter(book -> book.getExpireAfter().before(book.getEffectiveSince()))
                .map(PatchBook::getPid)
                .collect(Collectors.toList());
        if (invalidRangePids.isEmpty()) {
            return Collections.emptyList();
        }

        // not including date value in message currently because it is easy to check
        PatchBookValidationError error = new PatchBookValidationError();
        error.setMessage(INVALID_EFFECTIVE_DATE_RANGE_ERROR);
        error.setPatchBookIds(invalidRangePids);
        return Collections.singletonList(error);
    }

    /**
     * Validate whether there are patch items with invalid format in the given patch books
     *
     * NOTE no filtering will be performed in this method. it is suggested that all input {@link PatchBook} have the
     * same {@link PatchBook.Type}
     *
     * @param patchBooks list of patch books, should not be {@literal null}
     * @return a list of validation errors
     */
    public static List<PatchBookValidationError> validatePatchedItems(@NotNull List<PatchBook> patchBooks) {
        PatchBookValidationError error = new PatchBookValidationError();
        List<Long> patchedIds = new ArrayList<>();
        for (PatchBook patchBook : patchBooks) {
            Map<String,Object> patchItems = patchBook.getPatchItems();
            if (patchItems.isEmpty()) {
                if (!patchedIds.contains(patchBook.getPid())) {
                    patchedIds.add(patchBook.getPid());
                }
            }
            if (!patchedIds.isEmpty()) {
                boolean nullPatchItem = false;
                for (Map.Entry<String,Object> items : patchItems.entrySet()) { // checking if all values corresponding to keys are empty/null
                    if (items.getValue() == null) {
                        nullPatchItem = true;
                        break;
                    }
                }
                if (nullPatchItem) {
                    if (!patchedIds.contains(patchBook.getPid())) {
                        patchedIds.add(patchBook.getPid());
                    }
                }
            }
        }
        List<PatchBookValidationError> errorList = new ArrayList<>();
        if (patchedIds.size() > 0) {
            error.setMessage(INVALID_PATCH_ITEMS);
            error.setPatchBookIds(patchedIds);
            errorList.add(error);
        }
        return errorList;
    }

    /**
     * Check if the patch book already reaches end of life
     *
     * @param book target patch book, should not be {@literal null}
     * @param currentDate date object that represents current time, if null, a new date object will be created
     * @return true if the patch book DO reach EOF, false otherwise
     */
    public static boolean isEndOfLife(@NotNull PatchBook book, Date currentDate) {
        // checking current date
        currentDate = currentDate != null ? currentDate : new Date();
        if (book.getEffectiveSince() != null && currentDate.before(book.getEffectiveSince())) { // not effective yet
            return true;
        }
        if (book.getExpireAfter() != null && currentDate.after(book.getExpireAfter())) { // already expired
            return true;
        }
        return false;
    }

    /**
     * Determine whether the input {@link PatchBook} requires to patch AMLookup table
     *
     * @param book target patch book, should not be {@literal null}
     * @return true if we need to patch AMLookup table, false if no
     */
    public static boolean shouldPatchAMLookupTable(@NotNull PatchBook book) {
        Preconditions.checkNotNull(book);
        if (!PatchBook.Type.Lookup.equals(book.getType())) {
            return false;
        }
        String tupleStr = getMatchKeyValues(book).buildIdForKey();
        return AM_LOOKUP_PATCH_MATCH_KEYS.contains(tupleStr);
    }

    /**
     * Helper to create {@link PatchLog} from given {@link PatchBook}
     *
     * @param book target patch book, should not be {@literal null}
     * @return generated patch log with all related fields copied
     */
    public static PatchLog newPatchLog(@NotNull PatchBook book) {
        PatchLog log = new PatchLog();
        log.setPatchBookId(book.getPid());
        log.setPatchedValue(book.getPatchItems());
        log.setInputMatchKey(getMatchKeyValues(book));
        return log;
    }

    /**
     * Retrieve patch DUNS from {@link PatchBook}
     *
     * @param book input patch book
     * @return retrieved patch DUNS, {@literal null} if no patch domain in the book
     */
    public static String getPatchDuns(@NotNull PatchBook book) {
        return getStringPatchItem(book.getPatchItems(), MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS));
    }

    /**
     * Retrieve patch domain from {@link PatchBook}
     * @param book input patch book
     * @return retrieved patch domain, {@literal null} if no patch domain in the book
     */
    public static String getPatchDomain(@NotNull PatchBook book) {
        return getStringPatchItem(book.getPatchItems(), MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Domain));
    }

    /**
     * Check whether the provided match key is supported by the specific type of patch book
     *
     * @param patchBook target patch book
     * @return true if supported, false if not supported
     */
    public static boolean isMatchKeySupported(@NotNull PatchBook patchBook) {
        Preconditions.checkNotNull(patchBook);
        PatchBook.Type type = patchBook.getType();
        if (!SUPPORTED_MATCH_KEY_MAP.containsKey(type)) {
            String msg = String.format("PatchBook type=%s is not supported", type);
            throw new UnsupportedOperationException(msg);
        }

        MatchKeyTuple tuple = getMatchKeyValues(patchBook);
        Preconditions.checkNotNull(tuple);
        String tupleStr = tuple.buildIdForKey();
        for (String supportedTupleStr : SUPPORTED_MATCH_KEY_MAP.get(type)) {
            if (supportedTupleStr.equals(tupleStr)) {
                return true;
            }
        }
        return false;
    }



    /**
     * Helper method to retrieve the value of specified key from given patch items and cast it to {@link String}
     *
     * @param patchItems input patch items
     * @param key target key
     * @return retrieved value, {@literal null} if specified key does not exist or is not {@link String}
     */
    private static String getStringPatchItem(Map<String, Object> patchItems, @NotNull String key) {
        if (patchItems == null) {
            return null;
        }

        Object itemObj = patchItems.get(key);
        if (!(itemObj instanceof String)) {
            return null;
        }
        return (String) itemObj;
    }
}
