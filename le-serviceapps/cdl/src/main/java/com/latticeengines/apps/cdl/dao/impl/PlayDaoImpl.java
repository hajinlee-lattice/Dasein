package com.latticeengines.apps.cdl.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.PlayDao;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Play;

@Component("playDao")
public class PlayDaoImpl extends BaseDaoImpl<Play> implements PlayDao {

    @Override
    protected Class<Play> getEntityClass() {
        return Play.class;
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public Play findByName(String name, boolean considerDeleted) {

        Session session = getSessionFactory().getCurrentSession();

        Class<Play> entityClz = getEntityClass();
        String selectQueryStr = "FROM %s " //
                + "WHERE name = :name ";
        if (!considerDeleted) {
            selectQueryStr += "AND deleted = :deleted ";
        }

        selectQueryStr = String.format(selectQueryStr, entityClz.getSimpleName());

        Query<Play> query = session.createQuery(selectQueryStr);
        query.setParameter("name", name);
        if (!considerDeleted) {
            query.setBoolean("deleted", Boolean.FALSE);
        }

        Play play = query.uniqueResult();
        return play;
    }

    @Override
    public List<Play> findAllByRatingEnginePid(long pid) {
        return super.findAllByField("FK_RATING_ENGINE_ID", pid);
    }

    @SuppressWarnings({ "rawtypes", "deprecation" })
    @Override
    public List<String> findAllDeletedPlayIds(boolean forCleanupOnly) {
        Session session = getSessionFactory().getCurrentSession();

        Class<Play> entityClz = getEntityClass();
        String selectQueryStr = "SELECT name " //
                + "FROM %s " //
                + "WHERE deleted = :deleted ";

        if (forCleanupOnly) {
            selectQueryStr += "AND isCleanupDone = :isCleanupDone ";
        }

        selectQueryStr += "ORDER BY updated DESC ";

        selectQueryStr = String.format(selectQueryStr, entityClz.getSimpleName());

        Query query = session.createQuery(selectQueryStr);
        query.setBoolean("deleted", Boolean.TRUE);
        if (forCleanupOnly) {
            query.setBoolean("isCleanupDone", Boolean.FALSE);
        }

        List<?> rawResult = query.getResultList();

        if (CollectionUtils.isEmpty(rawResult)) {
            return new ArrayList<String>();
        }
        return JsonUtils.convertList(rawResult, String.class);
    }

}
