package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.ModelDao;
import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.exposed.domain.Model;

@Component("modelEntityMgr")
public class ModelEntityMgrImpl extends BaseEntityMgrImpl<Model> implements ModelEntityMgr {

    @Autowired
    private ModelDao modelDao;
    
    @Autowired
    private JobEntityMgr jobEntityMgr;
    
    public ModelEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<Model> getDao() {
        return modelDao;
    }
    
    @Override
    public void post(Model model) {
        List<Job> jobs = model.getJobs();
        
        for (Job job : jobs) {
            jobEntityMgr.post(job);
        }
        
        super.post(model);
    }
    
    @Override
    public void save() {
        jobEntityMgr.save();
        super.save();
    }
    
    
}
