package com.latticeengines.app.exposed.service.impl;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("metadataService")
public class MetadataServiceImpl implements MetadataService {
    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private AttributeCustomizationService attributeCustomizationService;

    @Override
    public List<ColumnMetadata> getAttributes(Integer offset, Integer max) {
        DataCollection dataCollection = getDataCollection();
        List<Table> tables = dataCollection.getTables();
        Stream<ColumnMetadata> stream = tables.stream() //
                .flatMap(t -> t.getAttributes().stream()) //
                .map(a -> a.getColumnMetadata()) //
                .sorted((a, b) -> a.getColumnId().compareTo(b.getColumnId()));

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

    private void personalize(List<ColumnMetadata> list) {
        attributeCustomizationService.addFlags(list.stream().map(c -> (HasAttributeCustomizations) c)
                .collect(Collectors.toList()));
    }

    @Override
    public Statistics getStatistics() {
        DataCollection dataCollection = getDataCollection();
        if (dataCollection != null) {
            StatisticsContainer container = dataCollection.getStatisticsContainer();
            if (container != null) {
                return container.getStatistics();
            }
        }
        return null;
    }

    private DataCollection getDataCollection() {
        return dataCollectionProxy.getDataCollectionByType(MultiTenantContext.getTenant().getId(),
                DataCollectionType.Segmentation);
    }
}
