package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.pls.service.DataLakeService;
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

    @Override
    public long getAttributesCount() {
        List<ColumnMetadata> cms = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.values()) {
            cms.addAll(getAttributesInEntity(entity));
        }
        return cms.size();
    }

    @Override
    public List<ColumnMetadata> getAttributes(Integer offset, Integer max) {
        List<ColumnMetadata> cms = new ArrayList<>();
        for (BusinessEntity entity : BusinessEntity.values()) {
            cms.addAll(getAttributesInEntity(entity));
        }
        Stream<ColumnMetadata> stream = cms.stream().sorted(Comparator.comparing(ColumnMetadata::getColumnId));
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

    private List<ColumnMetadata> getAttributesInEntity(BusinessEntity entity) {
        if (BusinessEntity.LatticeAccount.equals(entity)) {
            // it is cached in the proxy
            String currentDataCloudVersion = columnMetadataProxy.latestVersion(null).getVersion();
            List<ColumnMetadata> cms = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Segment,
                    currentDataCloudVersion);
            cms.forEach(cm -> cm.setEntity(entity));
            return cms;
        }
        String customerSpace = MultiTenantContext.getTenant().getId();
        TableRoleInCollection role = entity.getServingStore();
        Table batchTable = dataCollectionProxy.getTable(customerSpace, role);
        if (batchTable == null) {
            return Collections.emptyList();
        } else {
            List<ColumnMetadata> cms = batchTable.getAttributes().stream() //
                    .map(Attribute::getColumnMetadata) //
                    .collect(Collectors.toList());
            cms.forEach(cm -> cm.setEntity(entity));
            //TODO: should set category in metadata table
            if (BusinessEntity.Account.equals(entity)) {
                cms.forEach(cm -> cm.setCategory(Category.ACCOUNT_ATTRIBUTES));
            }
            return cms;
        }
    }

    private void personalize(List<ColumnMetadata> list) {
        attributeCustomizationService
                .addFlags(list.stream().map(c -> (HasAttributeCustomizations) c).collect(Collectors.toList()));
    }

    @Override
    public StatsCube getStatsCube() {
        Statistics statistics = getStatistics();
        if (statistics == null) {
            return null;
        }
        return StatsCubeUtils.toStatsCube(statistics);
    }

    @Override
    public TopNTree getTopNTree() {
        Statistics statistics = getStatistics();
        if (statistics == null) {
            return null;
        }
        return StatsCubeUtils.toTopNTree(statistics);
    }

    private Statistics getStatistics() {
        String customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId()).toString();
        StatisticsContainer container = dataCollectionProxy.getStats(customerSpace);
        if (container != null) {
            return container.getStatistics();
        }
        return null;
    }

}
