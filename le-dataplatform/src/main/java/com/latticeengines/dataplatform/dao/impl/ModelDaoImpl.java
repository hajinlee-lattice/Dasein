package com.latticeengines.dataplatform.dao.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.JobDao;
import com.latticeengines.dataplatform.dao.ModelDao;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.Model;

@Component("modelDao")
public class ModelDaoImpl extends BaseDaoImpl<Model> implements ModelDao {

    @Autowired
    private JobDao jobDao;
    
    public ModelDaoImpl() {
        super();
    }

    @Override
    public Model deserialize(String id, String content) {
        Model model = new Model();
        model.setId(id);
        
        if (content != null) {
            String[] jobIds = content.split(",");
            for (String jobId : jobIds) {
                Job job = new Job();
                job.setId(jobId);
                job.setModel(model);
                model.addJob(job);
            }
            return model;
        }
        return null;
    }

    @Override
    public String serialize(Model entity) {
        List<Job> jobs = entity.getJobs();
        StringBuilder sb = new StringBuilder();
        
        boolean first = true;
        for (Job job : jobs) {
            if (!first) {
                sb.append(","); 
            } else {
                first = false;
            }
            sb.append(job.getId());
        }
        return sb.toString();
    }
    
    @Override
    public void deleteStoreFile() {
        jobDao.deleteStoreFile();
        super.deleteStoreFile();
    }
    
    @Override
    public void clear() {
        jobDao.clear();
        super.clear();
    }
}
