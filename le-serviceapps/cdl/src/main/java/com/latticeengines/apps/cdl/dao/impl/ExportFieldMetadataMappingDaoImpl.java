package com.latticeengines.apps.cdl.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.ExportFieldMetadataMappingDao;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

@Component("exportFieldMetadataMappingDao")
public class ExportFieldMetadataMappingDaoImpl extends BaseDaoImpl<ExportFieldMetadataMapping> implements ExportFieldMetadataMappingDao {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    protected Class<ExportFieldMetadataMapping> getEntityClass() {
        return ExportFieldMetadataMapping.class;
    }

    @Override
    public List<ExportFieldMetadataMapping> updateExportFieldMetadataMappings(LookupIdMap lookupIdMap,
            List<ExportFieldMetadataMapping> exportFieldMetadataMappings) {

        if (lookupIdMap == null) {
            throw new LedpException(LedpCode.LEDP_40067);
        }

        List<ExportFieldMetadataMapping> retrievedFieldMapping = super.findAllByField("FK_LOOKUP_ID_MAP",
                lookupIdMap.getPid());

        retrievedFieldMapping.stream().forEach(fm -> {
            super.deleteByPid(fm.getPid(), true);
        });

        List<ExportFieldMetadataMapping> updatedExportFieldMappings = new ArrayList<ExportFieldMetadataMapping>();
        exportFieldMetadataMappings.stream().forEach(fm -> {
            ExportFieldMetadataMapping newMapping = new ExportFieldMetadataMapping();
            newMapping.setTenant(lookupIdMap.getTenant());
            newMapping.setLookupIdMap(lookupIdMap);
            newMapping.setSourceField(fm.getSourceField());
            newMapping.setDestinationField(fm.getDestinationField());
            newMapping.setOverwriteValue(fm.getOverwriteValue());
            updatedExportFieldMappings.add(newMapping);
        });

        log.info(JsonUtils.serialize(updatedExportFieldMappings));

        super.create(updatedExportFieldMappings, true);
        return updatedExportFieldMappings;
    }

}
