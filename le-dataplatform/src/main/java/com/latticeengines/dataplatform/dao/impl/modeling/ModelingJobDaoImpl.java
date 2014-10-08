package com.latticeengines.dataplatform.dao.impl.modeling;
//package com.latticeengines.dataplatform.dao.impl;
//
//import java.util.List;
//
//import org.hibernate.Criteria;
//import org.hibernate.Query;
//import org.hibernate.Session;
//import org.hibernate.criterion.Restrictions;
//import org.springframework.transaction.annotation.Propagation;
//import org.springframework.transaction.annotation.Transactional;
//
//import com.latticeengines.dataplatform.dao.ModelingJobDao;
//import com.latticeengines.domain.exposed.dataplatform.Job;
//import com.latticeengines.domain.exposed.modeling.ModelingJob;
//
//public class ModelingJobDaoImpl extends BaseDaoImpl<ModelingJob> implements ModelingJobDao {
//    public ModelingJobDaoImpl() {
//        super();
//    }
//
//    @Override
//    protected Class<ModelingJob> getEntityClass() {
//        return ModelingJob.class;
//    }
//
//    @Override
//    @Transactional(propagation = Propagation.REQUIRED)
//    public ModelingJob findByObjectId(String id) {
//        Session session = sessionFactory.getCurrentSession();
//        Query query = session.createQuery("from " + Job.class.getSimpleName() + " J where J.id=:aJobId");
//        query.setString("aJobId", id);
//        ModelingJob mj = (ModelingJob) query.uniqueResult();
//        return mj;
//    }
//
//    @Override
//    @Transactional(propagation = Propagation.REQUIRED)
//    public List<ModelingJob> findAllByObjectIds(List<String> ids) {
//        Session session = sessionFactory.getCurrentSession();
//        Criteria criteria = session.createCriteria(Job.class, "listAllByObjectIds");
//        criteria.add(Restrictions.in("id", ids));
//        criteria.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
//        List<ModelingJob> mjs = criteria.list();
//
//        return mjs;
//    }
//
//
//
//}
