package com.latticeengines.dataplatform.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Repository;

import com.latticeengines.dataplatform.dao.ModelDefinitionDao;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

@Repository("modelDefinitionDao")
public class ModelDefinitionDaoImpl extends BaseDaoImpl<ModelDefinition> implements ModelDefinitionDao {

    public ModelDefinitionDaoImpl() {
        super();
    }

    protected Class<ModelDefinition> getEntityClass() {
        return ModelDefinition.class;
    }

    @Override
    public ModelDefinition deserialize(String id, String content) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    /**
     * return 'null' if model definition is not found by name
     */
    public ModelDefinition findByName(String name) {
        Session session = sessionFactory.getCurrentSession();       
        Query query = session.createQuery("from " + ModelDefinition.class.getSimpleName() + " modelDef where modelDef.name=:aModelDefName");
        query.setString("aModelDefName", name);
        
        ModelDefinition modelDef = null;
        List list = query.list(); // uniqueResult();
        if(!list.isEmpty()) {
            modelDef = (ModelDefinition) list.get(0);
        }        
             
        return modelDef;
    }

}
