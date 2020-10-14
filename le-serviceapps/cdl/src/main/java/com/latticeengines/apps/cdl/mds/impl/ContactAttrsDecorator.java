package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

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

    private final Set<String> stdAttrs;

    private final Set<String> systemAttrs;

    private final Set<String> exportAttrs;

    private final boolean entityMatchEnabled;
    private final boolean onlyEntityMatchGAEnabled;

    ContactAttrsDecorator(boolean entityMatchEnabled, boolean onlyEntityMatchGAEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
        this.onlyEntityMatchGAEnabled = onlyEntityMatchGAEnabled;
        this.stdAttrs = SchemaRepository //
                .getStandardAttributes(BusinessEntity.Contact, entityMatchEnabled).stream() //
                .map(InterfaceName::name).collect(Collectors.toSet());
        this.systemAttrs = SchemaRepository //
                .getSystemAttributes(BusinessEntity.Contact, entityMatchEnabled).stream() //
                .map(InterfaceName::name).collect(Collectors.toSet());
        this.exportAttrs = SchemaRepository //
                .getDefaultExportAttributes(BusinessEntity.Contact, entityMatchEnabled).stream() //
                .map(InterfaceName::name).collect(Collectors.toSet());
    }

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
        return "contact-attrs";
    }

    private ColumnMetadata filter(ColumnMetadata cm) {
        if (BusinessEntity.Contact.equals(cm.getEntity())) {
            cm.setCategory(Category.CONTACT_ATTRIBUTES);
            cm.setAttrState(AttrState.Active);

            if (systemAttrs.contains(cm.getAttrName())) {
                return cm;
            }
            // DP-12913 if EntityMatchGA, hide all default system Ids
            if (StringUtils.isNotEmpty(cm.getAttrName()) && cm.getAttrName().startsWith("user_DefaultSystem_")
                    && onlyEntityMatchGAEnabled && (Category.SUB_CAT_CONTACT_IDS.equals(cm.getSubcategory()))) {
                cm.disableGroup(Segment);
                cm.disableGroup(Enrichment);
                cm.disableGroup(TalkingPoint);
                cm.disableGroup(CompanyProfile);
                cm.disableGroup(Model);
                cm.setCanSegment(false);
                cm.setCanModel(false);
                cm.setCanEnrich(false);
                cm.setAttrState(AttrState.Inactive);
                return cm;
            }

            if (InterfaceName.ContactId.name().equals(cm.getAttrName()) && entityMatchEnabled) {
                cm.disableGroup(Segment);
                cm.enableGroup(Enrichment);
                cm.disableGroup(TalkingPoint);
                cm.disableGroup(CompanyProfile);
                cm.disableGroup(Model);
                cm.setCanSegment(false);
                cm.setCanModel(false);
                if (onlyEntityMatchGAEnabled) {
                    cm.setCanEnrich(false);
                    cm.setAttrState(AttrState.Inactive);
                } else {
                    cm.setCanEnrich(true);
                    cm.setAttrState(AttrState.Active);
                }
                return cm;
            }

            // PLS-15406 setting for attributes corresponds to mappings in section
            // Unique ID, Other IDs, Match IDs, only enable for usage export
            if (InterfaceName.CustomerContactId.name().equals(cm.getAttrName()) && entityMatchEnabled) {
                cm.setDisplayName("Contact ID");
                cm.enableGroup(Enrichment);
                cm.disableGroup(TalkingPoint);
                cm.disableGroup(CompanyProfile);
                cm.disableGroup(Model);
                // do not use CustomerContactId in segment, as there will always be a system id that can be used
                // M36: still enable it. after asking all customer to move away from CustomerContactId, can disable it.
                cm.setCanSegment(true);
                cm.setCanModel(true);
                cm.setCanEnrich(true);
                if (onlyEntityMatchGAEnabled) {
                    cm.enableGroup(Segment);
                } else {
                    cm.disableGroup(Segment);
                }
                return cm;
            }

            cm.enableGroup(Segment);
            // enable some attributes for Export
            if (exportAttrs.contains(cm.getAttrName())) {
                cm.enableGroup(Enrichment);
            } else {
                cm.disableGroup(Enrichment);
            }
            cm.enableGroup(TalkingPoint);
            cm.disableGroup(CompanyProfile);
            cm.disableGroup(Model);
            if (stdAttrs.contains(cm.getAttrName())) {
                // a workaround due to Account Std and Contact Std attrs share AttrSpec
                cm.setCanModel(false);
            }
        }
        return cm;
    }

}
