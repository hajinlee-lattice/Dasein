package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.dao.PatchBookDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;

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

}
