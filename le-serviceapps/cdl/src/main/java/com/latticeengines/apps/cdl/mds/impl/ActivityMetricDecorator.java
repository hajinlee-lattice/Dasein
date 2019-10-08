package com.latticeengines.apps.cdl.mds.impl;


import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;

import java.util.Set;
import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class ActivityMetricDecorator implements Decorator {

    // all activity metric serving entities shares the system attrs
    private final Set<String> systemAttrs = SchemaRepository //
            .getSystemAttributes(BusinessEntity.WebVisitProfile, true).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        return metadata.map(this::filter);
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        return metadata.map(this::filter);
    }

    @Override
    public String getName() {
        return "activity-metric-attrs";
    }

    private ColumnMetadata filter(ColumnMetadata cm) {
        switch (cm.getEntity()) {
            case WebVisitProfile:
                cm.setCategory(Category.WEB_VISIT_PROFILE);
                break;
            default:
        }
        if (systemAttrs.contains(cm.getAttrName())) {
            return cm;
        }
        cm.enableGroup(Segment);
        cm.enableGroup(Enrichment);
        cm.disableGroup(Model);
        return cm;
    }

}
