package com.latticeengines.apps.lp.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.lp.dao.BucketMetadataDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

@Component("bucketMetadataDao")
public class BucketMetadataDaoImpl extends BaseDaoImpl<BucketMetadata> implements BucketMetadataDao {

    @Override
    protected Class<BucketMetadata> getEntityClass() {
        return BucketMetadata.class;
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public List<BucketMetadata> findBucketMetadatasForModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<BucketMetadata> entityClz = getEntityClass();
        String queryStr = String.format("from %s where MODEL_ID = :modelId", entityClz.getSimpleName());
        Query<BucketMetadata> query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        return query.list();
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public List<BucketMetadata> findUpToDateBucketMetadatasForModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<BucketMetadata> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where CREATION_TIMESTAMP = ( SELECT MAX(creationTimestamp) FROM %s WHERE MODEL_ID = :modelId ) AND MODEL_ID = :modelId",
                entityClz.getSimpleName(), entityClz.getSimpleName());
        Query<BucketMetadata> query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        return query.list();
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public List<BucketMetadata> findUpToDateBucketMetadatasForRatingEngine(Long ratingEnginePid) {
        Session session = getSessionFactory().getCurrentSession();
        Class<BucketMetadata> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where CREATION_TIMESTAMP = ( SELECT MAX(creationTimestamp) FROM %s WHERE RATING_ENGINE_ID = :ratingId ) AND RATING_ENGINE_ID = :ratingId",
                entityClz.getSimpleName(), entityClz.getSimpleName());
        Query<BucketMetadata> query = session.createQuery(queryStr);
        query.setLong("ratingId", ratingEnginePid);
        return query.list();
    }

}
