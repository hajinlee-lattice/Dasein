package com.latticeengines.datacloud.match.exposed.service;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.util.PatchBookUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;

/**
 * Validator for {@link PatchBook} entries
 */
public interface PatchBookValidator {

    /**
     * Error Messages for invalid patchItems in PatchBook
     */
    String PATCH_ITEM_NOT_IN_AM = "Invalid Patched Items provided. Column not present in LDC_ManageDB.AccountMasterColumn for current approved version :";
    String EXCLUDED_PATCH_ITEM = "Invalid Patched Items provided. Column present in excluded Patch list : ";

    /**
     * Error Messages for conflict in patchItems in patchBook
     */
    static final String CONFLICT_IN_PATCH_ITEM = "Conflict exists in provided patch items for match keys : ";

    static final String ATTRI_PATCH_DOM_DUNS_BASED_SRC_ERR = "Provided attribute is not from correct source : ";

    static final String ENCODED_ATTRS_NOT_SUPPORTED = "Encoded Attributes not supported : ";

    /**
     * Validate a list of {@link PatchBook} under specified DataCloud version. Only entries with the given type and
     * satisfy {@link PatchBookUtils#isEndOfLife(PatchBook, Date)} == FALSE (NOT end of life) will be validated.
     * Other fields will not be filtered, you should do that before passing in the list of patch books
     * (e.g., pass in only hotFix entries).
     *
     * @param type target patch book type, should not be {@literal null}
     * @param dataCloudVersion target DataCloud version, should not be {@literal null}
     * @param books list of patch books to be validated, should not be {@literal null}
     * @return {@link Pair} where left is the number of validated patch books, right contains list of errors to
     * describe which entries are invalid, both left and right will not be {@literal null}
     */
    Pair<Integer, List<PatchBookValidationError>> validate(
            @NotNull PatchBook.Type type, @NotNull String dataCloudVersion, @NotNull List<PatchBook> books);
}
