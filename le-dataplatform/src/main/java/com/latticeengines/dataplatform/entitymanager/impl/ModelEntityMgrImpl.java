package com.latticeengines.dataplatform.entitymanager.impl;

import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.BaseDao;
import com.latticeengines.dataplatform.dao.ModelDao;
import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.SequenceEntityMgr;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.Model;

@Component("modelEntityMgr")
public class ModelEntityMgrImpl extends BaseEntityMgrImpl<Model> implements ModelEntityMgr {

    @Autowired
    private ModelDao modelDao;
    
    @Autowired
    private JobEntityMgr jobEntityMgr;
    
    @Autowired
    private SequenceEntityMgr sequenceEntityMgr;
    
    public ModelEntityMgrImpl() {
        super();
    }

    @Override
    public BaseDao<Model> getDao() {
        return modelDao;
    }
    
    @Override
    public void post(Model model) {
        if (model.getId() == null) {
            model.setId(UUID.randomUUID().toString());
        }
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
