package com.latticeengines.propdata.match.service.impl;

import java.util.List;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.Predefined;
import com.latticeengines.propdata.match.service.ColumnMetadataService;
import com.latticeengines.propdata.match.util.MatchUtils;

@Component("columnMetadataServiceDispatch")
public class ColumnMetadataServiceDispatchImpl implements ColumnMetadataService {

    @Resource(name = "columnMetadataService")
    private ColumnMetadataService columnMetadataService;

    @Resource(name = "accountMasterColumnMetadataService")
    private ColumnMetadataService accountMasterColumnMetadataService;

    @Override
    public List<ColumnMetadata> fromPredefinedSelection(Predefined selectionName, String dataCloudVersion) {
        if (MatchUtils.isAccountMaster(dataCloudVersion)) {
            return accountMasterColumnMetadataService.fromPredefinedSelection(selectionName, dataCloudVersion);
        }
        return columnMetadataService.fromPredefinedSelection(selectionName, dataCloudVersion);

    }

    @Override
    public List<ColumnMetadata> fromSelection(ColumnSelection selection, String dataCloudVersion) {
        if (MatchUtils.isAccountMaster(dataCloudVersion)) {
            return accountMasterColumnMetadataService.fromSelection(selection, dataCloudVersion);
        }
        return columnMetadataService.fromSelection(selection, dataCloudVersion);
    }

    @Override
    public Schema getAvroSchema(Predefined selectionName, String recordName, String dataCloudVersion) {
        if (MatchUtils.isAccountMaster(dataCloudVersion)) {
            return accountMasterColumnMetadataService.getAvroSchema(selectionName, recordName, dataCloudVersion);
        }
        return columnMetadataService.getAvroSchema(selectionName, recordName, dataCloudVersion);
    }

}
