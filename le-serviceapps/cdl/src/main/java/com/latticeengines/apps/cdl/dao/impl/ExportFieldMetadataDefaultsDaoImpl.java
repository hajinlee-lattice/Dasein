package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import javax.persistence.TypedQuery;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ExportFieldMetadataDefaultsDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("exportFieldMetadataDefaultsDao")
public class ExportFieldMetadataDefaultsDaoImpl extends BaseDaoImpl<ExportFieldMetadataDefaults>
        implements ExportFieldMetadataDefaultsDao {

    @Override
    public List<ExportFieldMetadataDefaults> getAllDefaultExportFields(CDLExternalSystemName systemName) {
        return this.findAllByFields("externalSystemName", systemName);
    }

    @Override
    public List<ExportFieldMetadataDefaults> getHistoryEnabledDefaultFields(CDLExternalSystemName systemName) {
        return this.findAllByFields("externalSystemName", systemName, "historyEnabled", true);
    }

    @Override
    public List<ExportFieldMetadataDefaults> getExportEnabledDefaultFields(CDLExternalSystemName systemName) {
        return this.findAllByFields("externalSystemName", systemName, "exportEnabled", true);
    }

    @Override
    public List<ExportFieldMetadataDefaults> getExportEnabledDefaultFieldsForEntity(CDLExternalSystemName systemName,
            BusinessEntity entity) {
        return this.findAllByFields("externalSystemName", systemName, "exportEnabled", true, "entity", entity);
    }

    @Override
    public List<ExportFieldMetadataDefaults> getExportEnabledDefaultFieldsForAudienceType(CDLExternalSystemName systemName,
            AudienceType audienceType) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ExportFieldMetadataDefaults> entityClz = getEntityClass();
        String queryStr = String.format(
                "SELECT field FROM %s field JOIN field.audienceTypes audience WHERE field.externalSystemName=:systemName AND audience=:audienceType",
                entityClz.getSimpleName());
        TypedQuery<ExportFieldMetadataDefaults> q = session.createQuery(queryStr, ExportFieldMetadataDefaults.class);
        q.setParameter("systemName", systemName);
        q.setParameter("audienceType", audienceType);
        return q.getResultList();
    }

    @Override
    protected Class<ExportFieldMetadataDefaults> getEntityClass() {
        return ExportFieldMetadataDefaults.class;
    }

    @Override
    public void deleteBySystemName(CDLExternalSystemName systemName) {
        Session session = getSessionFactory().getCurrentSession();

        List<ExportFieldMetadataDefaults> metadataList = getAllDefaultExportFields(systemName);
        for (ExportFieldMetadataDefaults metadata : metadataList) {
            session.delete(metadata);
        }
    }

    @Override
    public void deleteByAttrNames(CDLExternalSystemName systemName, List<String> attrNames) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ExportFieldMetadataDefaults> entityClz = getEntityClass();
        String queryStr = String.format(
                "delete from  %s field where field.externalSystemName=:systemName and field.attrName in :attrNames",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("systemName", systemName);
        query.setParameter("attrNames", attrNames);
        query.executeUpdate();
    }
}
