package com.latticeengines.datacloud.match.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;

/**
 * Shared utilities for {@link PatchBook}
 */
public class PatchBookUtils {

    // TODO consider using equals & hashCode implementation for MatchKeyTuple, but probably too complicated
    // TODO currently using serialized format instead
    // Key: patch book type, Value: set of supported match key tuple (in serialized format)
    private static final Map<PatchBook.Type, Set<String>> SUPPORTED_MATCH_KEY_MAP = new HashMap<>();
    private static final String DUPLICATE_MATCH_KEY_ERROR = "Duplicate match key combination found : ";

    static {
        // TODO init supported match key tuple here
        // FIXME remove place holder
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
        MatchKeyTuple lookupTuple = new MatchKeyTuple.Builder() //
                .withDomain(MatchKey.Domain.name()) //
                .withDuns(MatchKey.DUNS.name()) //
                .withCountry(MatchKey.Country.name()) //
                .withState(MatchKey.State.name()) //
                .withCity(MatchKey.City.name()) //
                .withName(MatchKey.Name.name()) //
                .build(); //
        SUPPORTED_MATCH_KEY_MAP.put(PatchBook.Type.Lookup,
                new HashSet<>(Arrays.asList(lookupTuple.buildIdForKey())));
        MatchKeyTuple domainPatcherType = new MatchKeyTuple.Builder() //
                .withDuns(MatchKey.DUNS.name()) //
                .build(); //
        SUPPORTED_MATCH_KEY_MAP.put(PatchBook.Type.Domain,
                new HashSet<>(Arrays.asList(domainPatcherType.buildIdForKey())));
    }

    /**
     * Standardize all input match key fields of target patch book
     *
     * @param book target patch book, should not be {@literal null}
     */
    public static void standardize(@NotNull PatchBook book) {
    }

    /**
     * Generate a {@link MatchKeyTuple} that contains values of all match key fields in the given patch book
     *
     * @param book target patch book
     * @return generated tuple
     */
    public static MatchKeyTuple getMatchKeyValues(@NotNull PatchBook book) {
        return getMatchKey(book, true);
    }

    /**
     * Validate whether there are duplicate match key combination (same combination of match keys AND with the same
     *
     * values) in the given list of patch books
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

        List<PatchBookValidationError> errorList = matchKeyToPIDs.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1) // filter by key
                .map(temp -> {
                    PatchBookValidationError error = new PatchBookValidationError();
                    error.setMessage(DUPLICATE_MATCH_KEY_ERROR + temp.getKey());
                    error.setPatchBookIds(temp.getValue());
                    return error;
                }).collect(Collectors.toList());
        return errorList;
    }

    /**
     * Validate whether there are patch books that contain unsupported match key combination
     *
     * @param patchBooks list of patch books, should not be {@literal null}
     * @return a list of validation errors to show which patch books have unsupported match keys
     */
    public static List<PatchBookValidationError> validateMatchKeySupport(@NotNull List<PatchBook> patchBooks) {
        return null;
    }

    /**
     * Validate whether there are patch items with invalid format in the given patch books
     *
     * @param patchBooks list of patch books, should not be {@literal null}
     * @return a list of validation errors
     */
    public static List<PatchBookValidationError> validatePatchedItems(@NotNull List<PatchBook> patchBooks) {
        return null;
    }

    /**
     * Check if the patch book already reaches end of life
     *
     * @param book target patch book, should not be {@literal null}
     * @return true if the patch book DO reach EOF, false otherwise
     */
    public static boolean isEndOfLife(@NotNull PatchBook book) {
        return false;
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

        MatchKeyTuple tuple = getMatchKey(patchBook, false);
        Preconditions.checkNotNull(tuple);
        String tupleStr = tuple.buildIdForValue();
        for (String supportedTupleStr : SUPPORTED_MATCH_KEY_MAP.get(type)) {
            if (supportedTupleStr.equals(tupleStr)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Generate a match key tuple contains all fields present in the input patch book
     *
     * @param book input patch book, should not be null
     * @param useValue true to use values provided in the patch book, false to use corresponding {@link MatchKey}
     * @return generated tuple
     */
    private static MatchKeyTuple getMatchKey(@NotNull PatchBook book, boolean useValue) {
        Preconditions.checkNotNull(book);
        MatchKeyTuple tuple = new MatchKeyTuple();
        if (StringUtils.isNotBlank(book.getDomain())) {
            tuple.setDomain(useValue ? book.getDomain() : MatchKey.Domain.name());
        }
        if (StringUtils.isNotBlank(book.getDuns())) {
            tuple.setDuns(useValue ? book.getDuns() : MatchKey.DUNS.name());
        }
        if (StringUtils.isNotBlank(book.getName())) {
            tuple.setName(useValue ? book.getName() : MatchKey.Name.name());
        }
        if (StringUtils.isNotBlank(book.getCountry())) {
            tuple.setCountry(useValue ? book.getCountry() : MatchKey.Country.name());
        }
        if (StringUtils.isNotBlank(book.getState())) {
            tuple.setState(useValue ? book.getState() : MatchKey.State.name());
        }
        if (StringUtils.isNotBlank(book.getCity())) {
            tuple.setCity(useValue ? book.getCity() : MatchKey.City.name());
        }
        return tuple;
    }
}
