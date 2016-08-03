package com.latticeengines.propdata.match.service.impl;

import java.util.List;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.propdata.match.service.ColumnMetadataService;

@Component("columnMetadataServiceDispatch")
public class ColumnMetadataServiceDispatchImpl implements ColumnMetadataService {

    @Resource(name = "columnMetadataService")
    private ColumnMetadataService columnMetadataService;

    @Resource(name = "accountMasterColumnMetadataService")
    private ColumnMetadataService accountMasterColumnMetadataService;

    @Override
    public List<ColumnMetadata> fromPredefinedSelection(Predefined selectionName, String dataCloudVersion) {
        if (isVersionTwo(dataCloudVersion)) {
            return accountMasterColumnMetadataService.fromPredefinedSelection(selectionName, dataCloudVersion);
        }
        return columnMetadataService.fromPredefinedSelection(selectionName, dataCloudVersion);

    }

    private boolean isVersionTwo(String dataCloudVersion) {
        return StringUtils.isNotBlank(dataCloudVersion) && dataCloudVersion.startsWith("2.");
    }

    @Override
    public List<ColumnMetadata> fromSelection(ColumnSelection selection, String dataCloudVersion) {
        if (isVersionTwo(dataCloudVersion)) {
            accountMasterColumnMetadataService.fromSelection(selection, dataCloudVersion);
        }
        return columnMetadataService.fromSelection(selection, dataCloudVersion);
    }

    @Override
    public Schema getAvroSchema(Predefined selectionName, String recordName, String dataCloudVersion) {
        if (isVersionTwo(dataCloudVersion)) {
            accountMasterColumnMetadataService.getAvroSchema(selectionName, recordName, dataCloudVersion);
        }
        return columnMetadataService.getAvroSchema(selectionName, recordName, dataCloudVersion);
    }

}
