package com.latticeengines.pls.dao.impl;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Enrichment;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.pls.MarketoMatchFieldName;
import com.latticeengines.pls.dao.MarketoMatchFieldDao;

@Component("marketoMatchFieldDao")
public class MarketoMatchFieldDaoImpl extends BaseDaoImpl<MarketoMatchField>
        implements MarketoMatchFieldDao {

    @Override
    protected Class<MarketoMatchField> getEntityClass() {
        return MarketoMatchField.class;
    }

    @Override
    public void deleteMarketoMatchField(MarketoMatchFieldName fieldName, Enrichment enrichment) {
        Session session = getSessionFactory().getCurrentSession();
        Class<MarketoMatchField> entityClz = getEntityClass();
        String queryStr = String.format(
                "delete from %s where MARKETO_MATCH_FIELD_NAME = :marketoMatchFieldName and ENRICHMENT_Id = :enrichmentId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("marketoMatchFieldName", fieldName.toString());
        query.setString("enrichmentId", Long.toString(enrichment.getPid()));
        query.executeUpdate();
    }

}
