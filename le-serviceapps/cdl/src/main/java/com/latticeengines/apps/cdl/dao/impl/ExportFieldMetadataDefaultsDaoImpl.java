package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ExportFieldMetadataDefaultsDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataDefaults;

@Component("exportFieldMetadataDefaultsDao")
public class ExportFieldMetadataDefaultsDaoImpl extends BaseDaoImpl<ExportFieldMetadataDefaults>
        implements ExportFieldMetadataDefaultsDao {

    @Override
    public List<ExportFieldMetadataDefaults> getAllDefaultExportFields(CDLExternalSystemName systemName) {
        return this.findAllByField("externalSystemName", systemName);
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
    protected Class<ExportFieldMetadataDefaults> getEntityClass() {
        return ExportFieldMetadataDefaults.class;
    }

    @Override
    public void deleteBySystemName(CDLExternalSystemName systemName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ExportFieldMetadataDefaults> entityClz = getEntityClass();
        String queryStr = String.format(
                "delete from  %s field where field.externalSystemName=:systemName",
                entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("systemName", systemName);
        query.executeUpdate();
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

