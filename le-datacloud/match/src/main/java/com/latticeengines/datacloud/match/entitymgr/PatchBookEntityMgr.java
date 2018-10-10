package com.latticeengines.datacloud.match.entitymgr;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;

import java.util.List;

public interface PatchBookEntityMgr extends BaseEntityMgr<PatchBook> {

    /**
     *
     * @param pIds
     * @param hotFix
     */
    void setHotFix(@NotNull List<Long> pIds, boolean hotFix);

    /**
     *
     * @param pIds
     * @param endOfLife
     */
    void setEndOfLife(@NotNull List<Long> pIds, boolean endOfLife);

    /**
     *
     * @param pIds
     * @param version
     */
    void setEffectiveSinceVersion(@NotNull List<Long> pIds, String version);

    /**
     *
     * @param pIds
     * @param version
     */
    void setExpireAfterVersion(@NotNull List<Long> pIds, String version);
}
