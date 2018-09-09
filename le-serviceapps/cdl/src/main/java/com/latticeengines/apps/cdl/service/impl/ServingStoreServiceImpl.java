package com.latticeengines.apps.cdl.service.impl;

import java.util.Comparator;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.mds.CustomizedMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Service("servingStoreService")
public class ServingStoreServiceImpl implements ServingStoreService {

    @Inject
    private CustomizedMetadataStore customizedMetadataStore;

    @Inject
    private SystemMetadataStore systemMetadataStore;

    @Override
    public ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity, DataCollection.Version version) {
        return systemMetadataStore.getMetadataInParallel(entity, version);
    }

    @Override
    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity,
            DataCollection.Version version) {
        return customizedMetadataStore.getMetadataInParallel(entity, version).map(cm -> {
            cm.setBitOffset(null);
            cm.setNumBits(null);
            cm.setPhysicalName(null);
            cm.setStatisticalType(null);
            cm.setStats(null);
            cm.setDecodeStrategy(null);

            if (Boolean.TRUE.equals(cm.getShouldDeprecate()) && !AttrState.Inactive.equals(cm.getAttrState())) {
                cm.setAttrState(AttrState.Deprecated);
            }

            if (AttrState.Inactive.equals(cm.getAttrState())) {
                // disable these useages if it is inactive attribute.
                cm.disableGroup(ColumnSelection.Predefined.Segment);
                cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                cm.disableGroup(ColumnSelection.Predefined.TalkingPoint);
                cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                cm.disableGroup(ColumnSelection.Predefined.Model);
            }

            if (AttrState.Deprecated.equals(cm.getAttrState())) {
                // disable these useages if it is deprecated attribute.
                cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                cm.disableGroup(ColumnSelection.Predefined.Model);
            }

            return cm;
        });
    }

    @Override
    public Flux<ColumnMetadata> getFullyDecoratedMetadataInOrder(BusinessEntity entity,
            DataCollection.Version version) {
        return getFullyDecoratedMetadata(entity, version).sorted(Comparator.comparing(ColumnMetadata::getAttrName));
    }

}
