package com.latticeengines.datacloud.core.entitymgr;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;

import java.util.List;

public interface PatchBookEntityMgr extends BaseEntityMgr<PatchBook> {

    /**
     * Retrieve all {@link PatchBook} entries with specified {@link PatchBook.Type}
     *
     * @param type target patch book type, should not be {@literal null}
     * @return list of {@link PatchBook}, will not be {@literal null}
     */
    List<PatchBook> findByType(@NotNull PatchBook.Type type);

    /**
     * Retrieve all {@link PatchBook} entries with specified {@link PatchBook.Type} and {@link PatchBook#isHotFix()}
     *
     * @param type target patch book type, should not be {@literal null}
     * @param hotFix specified hot fix flag
     * @return list of {@link PatchBook}, will not be {@literal null}
     */
    List<PatchBook> findByTypeAndHotFix(@NotNull PatchBook.Type type, boolean hotFix);

    /**
     * Set hot fix flag for target {@link PatchBook}s identified by the given list of primary IDs
     *
     * @param pIds target list of patch book primary IDs, should not be {@literal null}
     * @param hotFix hot fix flag to set
     */
    void setHotFix(@NotNull List<Long> pIds, boolean hotFix);

    /**
     * Set end of life flag for target {@link PatchBook}s identified by the given list of primary IDs
     *
     * @param pIds target list of patch book primary IDs, should not be {@literal null}
     * @param endOfLife end of life flag to set
     */
    void setEndOfLife(@NotNull List<Long> pIds, boolean endOfLife);

    /**
     * Set {@link PatchBook#setEffectiveSinceVersion(String)} for target {@link PatchBook}s identified by
     * the given list of primary IDs
     *
     * @param pIds target list of patch book primary IDs, should not be {@literal null}
     * @param version version string to set, can be {@literal null}
     */
    void setEffectiveSinceVersion(@NotNull List<Long> pIds, String version);

    /**
     * Set {@link PatchBook#setExpireAfterVersion(String)} for target {@link PatchBook}s identified by
     * the given list of primary IDs
     *
     * @param pIds target list of patch book primary IDs, should not be {@literal null}
     * @param version version string to set, can be {@literal null}
     */
    void setExpireAfterVersion(@NotNull List<Long> pIds, String version);
}
