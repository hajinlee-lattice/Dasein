package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.util.PatchBookUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;

import java.util.Date;
import java.util.List;

/**
 * Validator for {@link PatchBook} entries
 */
public interface PatchBookValidator {
    /**
     * Validate a list of {@link PatchBook} under specified DataCloud version. Only entries with the given type and
     * satisfy {@link PatchBookUtils#isEndOfLife(PatchBook, Date)} == FALSE (NOT end of life) will be validated.
     * Other fields will not be filtered, you should do that before passing in the list of patch books
     * (e.g., pass in only hotFix entries).
     *
     * @param type target patch book type, should not be {@literal null}
     * @param dataCloudVersion target DataCloud version, should not be {@literal null}
     * @param books list of patch books to be validated, should not be {@literal null}
     * @return list of errors to describe which entries are invalid, will not be {@literal null}
     */
    List<PatchBookValidationError> validate(
            @NotNull PatchBook.Type type, @NotNull String dataCloudVersion, @NotNull List<PatchBook> books);
}
