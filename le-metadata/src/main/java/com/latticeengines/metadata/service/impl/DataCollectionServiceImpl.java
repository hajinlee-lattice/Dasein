package com.latticeengines.metadata.service.impl;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.entitymgr.TableTagEntityMgr;
import com.latticeengines.metadata.entitymgr.impl.DataCollectionCache;
import com.latticeengines.metadata.service.DataCollectionService;

@Component("dataCollectionService")
public class DataCollectionServiceImpl implements DataCollectionService {
    private static final Log log = LogFactory.getLog(DataCollectionServiceImpl.class);

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private TableEntityMgr tableEntityMgr;

    @Autowired
    private TableTagEntityMgr tagEntityMgr;

    @Autowired
    private DataCollectionCache dataCollectionCache;

    @Override
    public List<DataCollection> getDataCollections(String customerSpace) {
        return dataCollectionEntityMgr.findAll();
    }

    @Override
    public DataCollection getDataCollectionByType(String customerSpace, DataCollectionType type) {
        return dataCollectionCache.get(type);
    }

    @Override
    public DataCollection createOrUpdateDataCollection(String customerSpace, DataCollection dataCollection) {
        DataCollection existing = dataCollectionEntityMgr.getDataCollection(dataCollection.getName());
        if (existing != null) {
            dataCollectionEntityMgr.removeDataCollection(existing.getName());
        } else {
            dataCollection.setName("DataCollection_" + UUID.randomUUID());
        }

        dataCollectionEntityMgr.createDataCollection(dataCollection);
        dataCollectionCache.invalidate(dataCollection.getType());
        return dataCollectionCache.get(dataCollection.getType());
    }

    @Override
    public DataCollection upsertTableToCollection(String customerSpace, DataCollectionType type, String tableName,
            boolean purgeOldTable) {
        Table table = tableEntityMgr.findByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException(
                    "Cannot find table named " + tableName + " for customer " + customerSpace);
        }
        DataCollection dataCollection = getDataCollectionByType(customerSpace, type);
        if (dataCollection == null) {
            throw new IllegalArgumentException(
                    "Cannot find data collection of type " + type + " for customer " + customerSpace);
        }

        if (StringUtils.isBlank(table.getInterpretation())) {
            throw new IllegalArgumentException(
                    "Table to be added to a data collection must have schema interpretation.");
        }
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.valueOf(table.getInterpretation());
        Table oldTable = dataCollection.getTable(schemaInterpretation);
        if (oldTable != null) {
            String action = purgeOldTable ? "Untag and delete the old one." : "Untag the old one.";
            log.info("There is already a table of type " + schemaInterpretation + " in data collection "
                    + dataCollection.getName() + ". " + action);
            tagEntityMgr.untagTable(oldTable.getName(), dataCollection.getName());
            if (purgeOldTable) {
                tableEntityMgr.deleteByName(oldTable.getName());
            }
        }

        tagEntityMgr.tagTable(table, dataCollection.getName());
        log.info("Tag the table " + tableName + " of type " + schemaInterpretation + " to data collection "
                + dataCollection.getName());

        dataCollectionCache.invalidate(dataCollection.getType());
        return dataCollectionCache.get(dataCollection.getType());
    }

}
