package com.latticeengines.dataplatform.dao.impl.modeling;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.dao.modeling.ModelDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modeling.Model;

@Component("modelDao")
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
