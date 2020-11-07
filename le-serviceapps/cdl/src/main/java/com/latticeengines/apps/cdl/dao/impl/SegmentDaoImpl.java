package com.latticeengines.apps.cdl.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.SegmentDao;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;

@Component("segmentDao")
public class SegmentDaoImpl extends BaseDaoImpl<MetadataSegment> implements SegmentDao {

    @Override
    protected Class<MetadataSegment> getEntityClass() {
        return MetadataSegment.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public MetadataSegment findMasterSegment(String collectionName) {
        Session session = sessionFactory.getCurrentSession();
        Class<MetadataSegment> entityClz = getEntityClass();
        String queryPattern = "select seg from %s as seg";
        queryPattern += " join seg.dataCollection as dc";
        queryPattern += " where dc.name = :collectionName";
        queryPattern += " and seg.isMasterSegment is true";
        String queryStr = String.format(queryPattern, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (MetadataSegment) list.get(0);
    }

    @Override
    public MetadataSegment findByExternalInfo(MetadataSegment segment) {
        Session session = sessionFactory.getCurrentSession();
        Class<MetadataSegment> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s as segment where segment.listSegment.externalSystem  = :externalSystem " +
                        "and segment.listSegment.externalSegmentId = :externalSegmentId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("externalSystem", segment.getListSegment().getExternalSystem());
        query.setParameter("externalSegmentId", segment.getListSegment().getExternalSegmentId());
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (MetadataSegment) list.get(0);
        }
    }

    @SuppressWarnings({ "rawtypes" })
    @Override
    public List<String> getAllDeletedSegments() {
        Session session = getSessionFactory().getCurrentSession();

        Class<MetadataSegment> entityClz = getEntityClass();
        String selectQueryStr = "SELECT name " //
                + "FROM %s " //
                + "WHERE deleted = :deleted ";

        selectQueryStr += "ORDER BY updated DESC ";

        selectQueryStr = String.format(selectQueryStr, entityClz.getSimpleName());

        Query query = session.createQuery(selectQueryStr);
        query.setParameter("deleted", Boolean.TRUE);

        List<?> rawResult = query.getResultList();

        if (CollectionUtils.isEmpty(rawResult)) {
            return new ArrayList<String>();
        }
        return JsonUtils.convertList(rawResult, String.class);
    }

}
