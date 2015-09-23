package com.latticeengines.dataplatform.dao.impl.modeling;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.dao.modeling.ModelDefinitionDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;

@Component("modelDefinitionDao")
public class ModelDefinitionDaoImpl extends BaseDaoImpl<ModelDefinition> implements ModelDefinitionDao {


    public ModelDefinitionDaoImpl() {
        super();
    }

    protected Class<ModelDefinition> getEntityClass() {
        return ModelDefinition.class;
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
        List<?> list = query.list();
        if (!list.isEmpty()) {
            modelDef = (ModelDefinition) list.get(0);
        }

        return modelDef;
    }

}
