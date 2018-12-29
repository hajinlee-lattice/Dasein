package com.latticeengines.apps.cdl.mds.impl;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class SystemPostRenderDecorator implements Decorator {

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        return metadata.map(this::renderCm);
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        return metadata.map(this::renderCm);
    }

    private ColumnMetadata renderCm(ColumnMetadata cm) {
        if (Boolean.TRUE.equals(cm.getShouldDeprecate())) {
            String displayName = cm.getDisplayName();
            if (StringUtils.isNotBlank(displayName)) {
                cm.setDisplayName(displayName + " (DEPRECATED)");
            }
            // set Export init value to false, for deprecated attr
            cm.disableGroup(ColumnSelection.Predefined.Enrichment);
        }
        return cm;
    }

}
