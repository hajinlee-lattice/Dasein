package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.util.Set;
import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class ContactAttrsDecorator implements Decorator {

    private static Set<String> exportAttributes = SchemaRepository //
            .getDefaultExportAttributes(BusinessEntity.Contact).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    private static Set<String> stdAttrs = SchemaRepository //
            .getStandardAttributes(BusinessEntity.Contact).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        return metadata.map(ContactAttrsDecorator::staticFilter);
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        return metadata.map(ContactAttrsDecorator::staticFilter);
    }

    @Override
    public String getName() {
        return "contact-attrs";
    }

    private static ColumnMetadata staticFilter(ColumnMetadata cm) {
        if (BusinessEntity.Contact.equals(cm.getEntity())) {
            cm.setCategory(Category.CONTACT_ATTRIBUTES);
            cm.setAttrState(AttrState.Active);
            cm.enableGroup(Segment);
            // enable some attributes for Export
            if (exportAttributes.contains(cm.getAttrName())) {
                cm.enableGroup(Enrichment);
                cm.setCanEnrich(false);
            } else {
                cm.disableGroup(Enrichment);
            }
            cm.enableGroup(TalkingPoint);
            cm.disableGroup(CompanyProfile);
            cm.disableGroup(Model);
            if (stdAttrs.contains(cm.getAttrName())) {
                // a workaround due to Account Std and Contact Std attrs share
                // AttrSpec
                cm.setCanModel(false);
            }
        }
        return cm;
    }

}
