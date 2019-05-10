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

    private final Set<String> systemAttrs;

    private final Set<String> internalLookupIdAttrs;

    private static final Set<String> exportAttrs = SchemaRepository //
            .getDefaultExportAttributes(BusinessEntity.Account).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    private final boolean internalEnrichEnabled;

    AccountAttrsDecorator(boolean internalEnrichEnabled, boolean entityMatchEnabled) {
        this.internalEnrichEnabled = internalEnrichEnabled;
        this.systemAttrs = SchemaRepository //
                .getSystemAttributes(BusinessEntity.Account, entityMatchEnabled).stream() //
                .map(InterfaceName::name).collect(Collectors.toSet());
        this.internalLookupIdAttrs = SchemaRepository //
                .getInternalLookupIdAttributes(BusinessEntity.Account, entityMatchEnabled).stream() //
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
        return "account-attrs";
    }

    private ColumnMetadata filter(ColumnMetadata cm) {
        return isCustomerAttr(cm) ? filterCustomerAttr(cm) : filterLdcAttr(cm);
    }

    private ColumnMetadata filterCustomerAttr(ColumnMetadata cm) {
        cm.setCategory(Category.ACCOUNT_ATTRIBUTES);
        cm.setAttrState(AttrState.Active);

        if (systemAttrs.contains(cm.getAttrName())) {
            return cm;
        }

        if (InterfaceName.AccountId.name().equalsIgnoreCase(cm.getAttrName())
                || InterfaceName.CustomerAccountId.name().equalsIgnoreCase(cm.getAttrName())) {
            cm.setSubcategory("Account IDs");
        }

        if (internalLookupIdAttrs.contains(cm.getAttrName())) {
            cm.enableGroup(Enrichment);
            cm.disableGroup(Segment);
            cm.disableGroup(Model);
            cm.disableGroup(TalkingPoint);
            cm.disableGroup(CompanyProfile);
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
