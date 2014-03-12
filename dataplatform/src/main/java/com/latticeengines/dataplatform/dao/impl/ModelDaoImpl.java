package com.latticeengines.dataplatform.dao.impl;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.JobDao;
import com.latticeengines.dataplatform.dao.ModelDao;
import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.exposed.domain.Model;

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
        model.setId(Integer.parseInt(id));
        
        if (content != null) {
            String[] jobIds = content.split(",");
            for (String jobId : jobIds) {
                Job job = jobDao.deserialize(jobId, null);
                job.setModel(model);
            }
            return model;
        }
        return null;
    }

    @Override
    public String serialize(Model entity) {
        List<Job> jobs = entity.getJobs();
        String value = StringUtils.join(jobs, ",");
        return value;
    }
    
    @Override
    public void deleteStoreFile() {
        jobDao.deleteStoreFile();
        super.deleteStoreFile();
    }
    

}
