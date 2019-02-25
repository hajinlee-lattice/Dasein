package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.pls.dao.MetadataSegmentExportDao;

@Component("metadataSegmentExportDao")
public class MetadataSegmentExportDaoImpl extends BaseDaoImpl<MetadataSegmentExport>
        implements MetadataSegmentExportDao {

    @Override
    protected Class<MetadataSegmentExport> getEntityClass() {
        return MetadataSegmentExport.class;
    }

    @Override
    public MetadataSegmentExport findByExportId(String exportId) {
        if (StringUtils.isBlank(exportId)) {
            return null;
        }

        Session session = getSessionFactory().getCurrentSession();
        Class<MetadataSegmentExport> entityClz = getEntityClass();
        String queryStr = String.format(
                " FROM %s " //
                        + " WHERE exportId = :exportId ", //
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("exportId", exportId);
        List<?> list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (MetadataSegmentExport) list.get(0);
    }

    @Override
    public void deleteByExportId(String exportId) {
        MetadataSegmentExport entity = findByExportId(exportId);
        if (entity != null) {
            super.delete(entity);
        }
    }

}
