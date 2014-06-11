package com.latticeengines.dataplatform.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.ModelDao;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.Model;

@Repository("modelDao")
public class ModelDaoImpl extends BaseDaoImpl<Model> implements ModelDao {

  
    public ModelDaoImpl() {
        super();
    }

    protected Class<Model> getEntityClass() {
        return Model.class;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public Model findByObjectId(String id) {
        Session session = sessionFactory.getCurrentSession();
        Query query = session.createQuery("from " + Model.class.getSimpleName() + " model where model.id=:aModelId");
        query.setString("aModelId", id);
        Model model = (Model) query.uniqueResult();
                
        return model;
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

    
}
