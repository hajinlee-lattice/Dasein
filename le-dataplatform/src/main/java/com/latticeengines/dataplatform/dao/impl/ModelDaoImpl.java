package com.latticeengines.dataplatform.dao.impl;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.ModelDao;
import com.latticeengines.domain.exposed.dataplatform.Model;

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

}
