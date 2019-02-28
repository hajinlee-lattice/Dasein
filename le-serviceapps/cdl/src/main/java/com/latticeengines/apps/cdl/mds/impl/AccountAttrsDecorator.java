package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;


public class AccountAttrsDecorator implements Decorator {

    private static final Set<String> systemAttributes = SchemaRepository //
            .getSystemAttributes(BusinessEntity.Account).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    private static Set<String> exportAttributes = SchemaRepository //
            .getDefaultExportAttributes(BusinessEntity.Account).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    private final boolean internalEnrichEnabled;

    AccountAttrsDecorator(boolean internalEnrichEnabled) {
        this.internalEnrichEnabled = internalEnrichEnabled;
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
        return "account-attrs";
    }

    private ColumnMetadata filter(ColumnMetadata cm) {
        return isCustomerAttr(cm) ? filterCustomerAttr(cm) : filterLdcAttr(cm);
    }

    private ColumnMetadata filterCustomerAttr(ColumnMetadata cm) {
        cm.setCategory(Category.ACCOUNT_ATTRIBUTES);
        cm.setAttrState(AttrState.Active);

        if (systemAttributes.contains(cm.getAttrName())) {
            return cm;
        }

        cm.enableGroup(Segment);
        // enable some attributes for Export
        if (exportAttributes.contains(cm.getAttrName())) {
            cm.enableGroup(Enrichment);
        } else {
            cm.disableGroup(Enrichment);
        }
        cm.enableGroup(TalkingPoint);
        cm.disableGroup(CompanyProfile);
        cm.disableGroup(Model);

        if (InterfaceName.AccountId.name().equalsIgnoreCase(cm.getAttrName())) {
            cm.setSubcategory("Account IDs");
        }

        //TODO: should move allow change part to attr-specification
        // disable date attributes to be used in modeling
        if (LogicalDataType.Date.equals(cm.getLogicalDataType())) {
            cm.setCanModel(false);
        }
        return cm;
    }

    private ColumnMetadata filterLdcAttr(ColumnMetadata cm) {
        // Internal enrich attrs
        if (!internalEnrichEnabled && Boolean.TRUE.equals(cm.getCanInternalEnrich())) {
            cm.disableGroup(Enrichment);
            cm.disableGroup(TalkingPoint);
            cm.disableGroup(CompanyProfile);
            cm.setCanEnrich(false);
            return cm;
        }

        // Set initial value of Export
        if (Boolean.TRUE.equals(cm.getCanEnrich())
                && (Category.FIRMOGRAPHICS.equals(cm.getCategory())
                || StringUtils.isNotBlank(cm.getDataLicense()))) {
            cm.enableGroup(Enrichment);
        } else {
            cm.disableGroup(Enrichment);
        }

        // Further tweak on can Enrich flag
        if (!Boolean.TRUE.equals(cm.getCanEnrich())) {
            cm.disableGroup(Enrichment);
            cm.disableGroup(TalkingPoint);
            cm.disableGroup(CompanyProfile);
        }

        return cm;
    }

    private static boolean isCustomerAttr(ColumnMetadata cm) {
        return cm.getTagList() == null || !CollectionUtils.containsAny(cm.getTagList(), Tag.EXTERNAL);
    }
}
