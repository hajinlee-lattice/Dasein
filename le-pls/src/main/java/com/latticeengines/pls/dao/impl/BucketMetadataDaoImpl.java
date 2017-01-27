package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.pls.dao.BucketMetadataDao;

@Component("bucketMetadataDao")
public class BucketMetadataDaoImpl extends BaseDaoImpl<BucketMetadata>
        implements BucketMetadataDao {

    @Override
    protected Class<BucketMetadata> getEntityClass() {
        return BucketMetadata.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<BucketMetadata> findBucketMetadatasForModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<BucketMetadata> entityClz = getEntityClass();
        String queryStr = String.format("from %s where MODEL_ID = :modelId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<BucketMetadata> findUpToDateBucketMetadatasForModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<BucketMetadata> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where CREATION_TIMESTAMP = ( SELECT MAX(creationTimestamp) FROM %s WHERE MODEL_ID = :modelId ) AND MODEL_ID = :modelId",
                entityClz.getSimpleName(), entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        return query.list();
    }

}
