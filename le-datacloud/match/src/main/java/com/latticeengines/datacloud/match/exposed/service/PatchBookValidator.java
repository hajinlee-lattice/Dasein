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
     * Error Messages for : type = Attribute and Cleanup = 1
     */
    String ERR_ATTRI_CLEANUP = "Cannot cleanup PatchBook Type : Attribute. Error for Ids : ";

    /**
     * Error Messages for conflict in patchItems in patchBook
     */
    String CONFLICT_IN_PATCH_ITEM = "Conflict exists in provided patch items for match keys : ";

    String ATTRI_PATCH_DOM_BASED_SRC_ERR = "Cannot patch domain source attributes : ";

    String ATTRI_PATCH_DUNS_BASED_SRC_ERR = "Cannot patch duns source attributes : ";

    String DOMAIN_PATCH_MATCH_KEY_ERR = "Provided Match Key/Patch item attribute is incorrect. "
            + "Expected input match key is DUNS and patched Item attribute is Domain.";

    String ERR_IN_PATCH_ITEMS = "Allowed Patch Item is only Domain Attribute.";

    String DUPLI_MATCH_KEY_AND_PATCH_ITEM_COMBO = "Duplicate combination of Match Key DUNS and Patch Item Domain exists : ";

    String ENCODED_ATTRS_NOT_SUPPORTED = "Encoded Attributes not supported : ";

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
