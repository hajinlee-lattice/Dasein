package com.latticeengines.app.exposed.service.impl;

import java.io.InputStream;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("dataLakeService")
public class DataLakeServiceImpl implements DataLakeService {

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private AttributeCustomizationService attributeCustomizationService;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    private Statistics demoStats;
    private List<ColumnMetadata> demoAccountAttributes;

    // TODO: also need to add AM attrs
    @Override
    public List<ColumnMetadata> getAttributes(Integer offset, Integer max) {
        String customerSpace = MultiTenantContext.getTenant().getId();
        DataCollection dataCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace);
        // TODO:     this is not right, there many tables in data collection that
        // TODO:     are not meant to be surfaced, e.g. ImportTable for data feed.
        List<Table> tables = dataCollectionProxy.getAllTables(customerSpace, dataCollection.getName());
        Stream<ColumnMetadata> stream = tables.stream() //
                .flatMap(t -> t.getAttributes().stream()) //
                .map(Attribute::getColumnMetadata) //
                .sorted(Comparator.comparing(ColumnMetadata::getColumnId));
        try {
            if (offset != null) {
                stream = stream.skip(offset);
            }
            if (max != null) {
                stream = stream.limit(max);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18143);
        }
        List<ColumnMetadata> list = stream.collect(Collectors.toList());
        personalize(list);
        return list;
    }

    @Override
    public List<ColumnMetadata> getAttributesInEntity(BusinessEntity entity) {
        if (BusinessEntity.LatticeAccount.equals(entity)) {
            // it is cached in the proxy
            String currentDataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
            return columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Segment, currentDataCloudVersion);
        }
        String customerSpace = MultiTenantContext.getTenant().getId();
        TableRoleInCollection role = entity.getServingStore();
        Table batchTable = dataCollectionProxy.getTable(customerSpace, role);
        Stream<ColumnMetadata> stream = batchTable.getAttributes().stream() //
                .map(Attribute::getColumnMetadata) //
                .sorted(Comparator.comparing(ColumnMetadata::getColumnId));
        List<ColumnMetadata> list = stream.collect(Collectors.toList());
        personalize(list);
        return list;
    }

    private void personalize(List<ColumnMetadata> list) {
        attributeCustomizationService
                .addFlags(list.stream().map(c -> (HasAttributeCustomizations) c).collect(Collectors.toList()));
    }

    @Override
    public Statistics getStatistics() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        StatisticsContainer container = dataCollectionProxy.getStats(customerSpace);
        if (container != null) {
            return container.getStatistics();
        }
        return null;
    }

    @Override
    public Statistics getDemoStatistics() {
        if (demoStats == null) {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("stats.json");
            ObjectMapper om = new ObjectMapper();
            try {
                demoStats = om.readValue(is, Statistics.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse json resource.", e);
            }
        }
        return demoStats;
    }

    @Override
    public List<ColumnMetadata> getDemoAttributes(BusinessEntity entity) {
        if (BusinessEntity.LatticeAccount.equals(entity)) {
            // it is cached in the proxy
            String currentDataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
            return columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Segment, currentDataCloudVersion);
        }
        if (demoAccountAttributes == null) {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("cm.json");
            ObjectMapper om = new ObjectMapper();
            try {
                demoAccountAttributes = om.readValue(is, new TypeReference<List<ColumnMetadata>>() {});
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse json resource.", e);
            }
        }
        return demoAccountAttributes;
    }

}
