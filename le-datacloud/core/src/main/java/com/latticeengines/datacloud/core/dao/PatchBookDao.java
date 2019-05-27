package com.latticeengines.datacloud.core.dao;

import java.util.List;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;

public interface PatchBookDao extends BaseDao<PatchBook> {
    /**
     * Update specified field of all the entities that have primary ID in the given list to the given value
     * @param pIds given list of primary IDs
     * @param fieldName field name, should not be {@literal null}
     * @param value value to be set, nullable
     */
    void updateField(@NotNull List<Long> pIds, @NotNull String fieldName, Object value);

    List<PatchBook> findAllByField(int offset, int limit, String sortByField,
            Object... fieldAndValues);
}
