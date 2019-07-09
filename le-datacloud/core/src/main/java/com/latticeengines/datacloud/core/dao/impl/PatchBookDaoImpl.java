package com.latticeengines.datacloud.core.dao.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.query.Query;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.dao.PatchBookDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook.Type;

public class PatchBookDaoImpl
        extends BaseDaoWithAssignedSessionFactoryImpl<PatchBook> implements PatchBookDao {

    private static final String PID_LIST_PARAMETER_NAME = "pIds";

    @Override
    protected Class<PatchBook> getEntityClass() {
        return PatchBook.class;
    }

    @Override
    public void updateField(@NotNull List<Long> pIds, @NotNull String fieldName, Object value) {
        Preconditions.checkNotNull(pIds);
        Preconditions.checkNotNull(fieldName);
        Session session = getCurrentSession();
        String queryStr = getUpdateFieldQueryStr(fieldName);
        Query<?> query = session.createQuery(queryStr);
        query.setParameter(fieldName, value);
        query.setParameterList(PID_LIST_PARAMETER_NAME, pIds);
        query.executeUpdate();
    }

    /*
     * update fieldName for all PatchBook that has a PID in a list
     */
    private String getUpdateFieldQueryStr(@NotNull String fieldName) {
        return String.format("UPDATE %s SET %s = :%s WHERE %s IN (:%s)",
                PatchBook.class.getSimpleName(), fieldName, fieldName, PatchBook.COLUMN_PID, PID_LIST_PARAMETER_NAME);
    }

    /*
     * get min and max pid from PatchBook
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Long> getMinMaxPid(@NotNull Type type) {
        Preconditions.checkNotNull(type);
        String fieldName = PatchBook.COLUMN_PID.toLowerCase();
        Session session = getCurrentSession();
        String minMaxPidQueryStr = String.format(
                "SELECT new map (min(%s) as MIN, max(%s) as MAX) FROM %s WHERE Type = '%s'",
                fieldName, fieldName,
                getEntityClass().getSimpleName(),
                type.name());
        Query<?> minMaxPidQuery = session.createQuery(minMaxPidQueryStr);
        Map<String, Long> minMaxPid = (Map<String, Long>) minMaxPidQuery.list().get(0);
        Map<String, Long> result = new HashMap<>();
        result.put(MIN_PID, minMaxPid.get(MIN_PID));
        result.put(MAX_PID, minMaxPid.get(MAX_PID));
        return result;
    }

}
