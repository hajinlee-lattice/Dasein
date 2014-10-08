package com.latticeengines.dataplatform.entitymanager.impl.modeling;
//package com.latticeengines.dataplatform.entitymanager.impl;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Component;
//import org.springframework.transaction.annotation.Propagation;
//import org.springframework.transaction.annotation.Transactional;
//
//import com.latticeengines.dataplatform.dao.BaseDao;
//import com.latticeengines.dataplatform.dao.JobDao;
//import com.latticeengines.dataplatform.dao.ModelingJobDao;
//import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
//import com.latticeengines.dataplatform.entitymanager.ModelingJobEntityMgr;
//import com.latticeengines.domain.exposed.dataplatform.Job;
//import com.latticeengines.domain.exposed.modeling.ModelingJob;
//
//@Component("modelingJobEntityMgr")
//public class ModelingJobEntityMgrImpl extends BaseEntityMgrImpl<ModelingJob> implements ModelingJobEntityMgr {
//
//    @Autowired
//    private ModelingJobDao modelingJobDao;
//
//    public ModelingJobEntityMgrImpl() {
//        super();
//    }
//
//    @Override
//    public BaseDao<ModelingJob> getDao() {
//        return modelingJobDao;
//    }
//
//    @Override
//    @Transactional(propagation = Propagation.REQUIRED)
//    public ModelingJob findByObjectId(String id) {
//        return modelingJobDao.findByObjectId(id);
//    }
//
//    /**
//     * find all Jobs by its object id (JobId)
//     * 
//     * @param <a>
//     *            jobIds - job ids to find by. If argument is empty or null, a empty set is returned.
//     * @return - jobs satisfying the jobids querying condition; empty Set if nothing is found.
//     * 
//     * 
//     */
//    @Override
//    @Transactional(propagation = Propagation.REQUIRED)
//    public List<ModelingJob> findAllByObjectIds(List<String> ids) {
//        if (ids == null || ids.isEmpty()) {
//            return new ArrayList<ModelingJob>();
//        }
//
//        return modelingJobDao.findAllByObjectIds(ids);
//    }
//}
