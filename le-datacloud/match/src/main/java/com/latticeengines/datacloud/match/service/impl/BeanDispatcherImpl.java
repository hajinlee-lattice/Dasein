package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.exposed.service.BeanDispatcher;
import com.latticeengines.datacloud.match.exposed.service.ColumnMetadataService;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

@SuppressWarnings("rawtypes")
@Component("beanDispatcher")
public class BeanDispatcherImpl implements BeanDispatcher {

    @Autowired
    private List<DbHelper> dbHelpers;

    @Autowired
    private List<ColumnSelectionService> columnSelectionServices;

    @Autowired
    private List<ColumnMetadataService> columnMetadataServices;

    @Autowired
    private List<MetadataColumnService> metadataColumnServices;

    public DbHelper getDbHelper(String dataCloudVersion) {
        for (DbHelper helper : dbHelpers) {
            if (helper.accept(dataCloudVersion)) {
                return helper;
            }
        }
        throw new RuntimeException("Cannot find a DbHelper for the data cloud version " + dataCloudVersion);
    }

    public DbHelper getDbHelper(MatchContext context) {
        return getDbHelper(getDataCloudVersionFromMatchContext(context));
    }

    public ColumnSelectionService getColumnSelectionService(String dataCloudVersion) {
        for (ColumnSelectionService service : columnSelectionServices) {
            if (service.accept(dataCloudVersion)) {
                return service;
            }
        }
        throw new RuntimeException(
                "Cannot find a ColumnSelectionService for the data cloud version " + dataCloudVersion);
    }

    public ColumnSelectionService getColumnSelectionService(MatchContext context) {
        return getColumnSelectionService(getDataCloudVersionFromMatchContext(context));
    }

    public ColumnMetadataService getColumnMetadataService(String dataCloudVersion) {
        for (ColumnMetadataService service : columnMetadataServices) {
            if (service.accept(dataCloudVersion)) {
                return service;
            }
        }
        throw new RuntimeException(
                "Cannot find a ColumnMetadataService for the data cloud version " + dataCloudVersion);
    }

    public MetadataColumnService getMetadataColumnService(String dataCloudVersion) {
        for (MetadataColumnService service : metadataColumnServices) {
            if (service.accept(dataCloudVersion)) {
                return service;
            }
        }
        throw new RuntimeException(
                "Cannot find a MetadataColumnService for the data cloud version " + dataCloudVersion);
    }

    public MetadataColumnService getMetadataColumnService(MatchContext context) {
        return getMetadataColumnService(getDataCloudVersionFromMatchContext(context));
    }

    private String getDataCloudVersionFromMatchInput(MatchInput input) {
        return input.getDataCloudVersion();

    }

    private String getDataCloudVersionFromMatchContext(MatchContext context) {
        MatchInput input = context.getInput();
        if (input == null) {
            throw new NullPointerException("Cannot find a MatchInput in MatchContext");
        }
        return getDataCloudVersionFromMatchInput(input);
    }

}
