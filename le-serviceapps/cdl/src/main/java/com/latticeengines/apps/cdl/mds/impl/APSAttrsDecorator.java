package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.util.Set;
import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class APSAttrsDecorator implements Decorator {

    private static final Set<String> systemAttributes = SchemaRepository
            .getSystemAttributes(BusinessEntity.AnalyticPurchaseState, false).stream().map(InterfaceName::name)
            .collect(Collectors.toSet());

    @Override
    public String getName() {
        return "aps-attrs";
    }

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        return metadata.map(APSAttrsDecorator::staticFilter);
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        return metadata.map(APSAttrsDecorator::staticFilter);
    }

    static ColumnMetadata staticFilter(ColumnMetadata cm) {
        if (BusinessEntity.AnalyticPurchaseState.equals(cm.getEntity())) {
            cm.setAttrState(AttrState.Active);
            if (systemAttributes.contains(cm.getAttrName())) {
                cm.setCanModel(false);
                return cm;
            }
            cm.disableGroup(Segment);
            cm.disableGroup(Enrichment);
            cm.disableGroup(TalkingPoint);
            cm.disableGroup(CompanyProfile);
            cm.enableGroup(Model);
            cm.setCanModel(true);
        }
        return cm;
    }

}
