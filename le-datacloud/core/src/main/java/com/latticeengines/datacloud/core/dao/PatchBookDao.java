package com.latticeengines.datacloud.core.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook.Type;

public interface PatchBookDao extends BaseDao<PatchBook> {
    /**
     * Update specified field of all the entities that have primary ID in the given list to the given value
     * @param pIds given list of primary IDs
     * @param fieldName field name, should not be {@literal null}
     * @param value value to be set, nullable
     */
    public static final String MIN_PID = "MIN";
    public static final String MAX_PID = "MAX";

    void updateField(@NotNull List<Long> pIds, @NotNull String fieldName, Object value);

    Map<String, Long> getMinMaxPid(@NotNull Type type, @NotNull String pidColumn);

}
