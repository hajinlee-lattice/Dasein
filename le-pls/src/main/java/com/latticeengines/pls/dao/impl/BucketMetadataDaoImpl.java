package com.latticeengines.pls.dao.impl;

import java.util.List;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import org.hibernate.Query;
import org.hibernate.Session;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.pls.dao.BucketMetadataDao;
import org.springframework.stereotype.Component;

@Component("bucketMetadataDao")
public class BucketMetadataDaoImpl extends BaseDaoImpl<BucketMetadata> implements BucketMetadataDao {

    @Override
    protected Class<BucketMetadata> getEntityClass() {
        return BucketMetadata.class;
    }


    @SuppressWarnings("unchecked")
    @Override
    public List<BucketMetadata> findBucketMetadatasForModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<BucketMetadata> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where MODEL_SUMMARY_ID = :modelId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelId", modelId);
        return query.list();
    }

}
