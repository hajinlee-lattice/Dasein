package com.latticeengines.datacloud.match.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.match.exposed.service.PatchBookValidator;
import com.latticeengines.datacloud.match.util.PatchBookUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component("patchBookValidator")
public class PatchBookValidatorImpl implements PatchBookValidator {

    @Inject
    private CountryCodeService countryCodeService;

    @Override
    public List<PatchBookValidationError> validate(
            @NotNull PatchBook.Type type, @NotNull String dataCloudVersion, @NotNull List<PatchBook> books) {
        Preconditions.checkNotNull(books);
        Preconditions.checkNotNull(type);
        if (books.isEmpty()) {
            return Collections.emptyList();
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

        return errors;
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
        // TODO remember to validate duplicate match key
        // TODO implement
        return Collections.emptyList();
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
