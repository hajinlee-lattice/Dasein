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

    private final Set<String> exportAttrs;

    private Set<String> attrNameInOtherIDAndMatchID;

    private final boolean internalEnrichEnabled;
    private final boolean entityMatchEnabled;
    private final boolean onlyEntityMatchGAEnabled;

    AccountAttrsDecorator(boolean internalEnrichEnabled, boolean entityMatchEnabled, boolean onlyEntityMatchGAEnabled,
                          Set<String> attrNameInOtherIDAndMatchID) {
        this.internalEnrichEnabled = internalEnrichEnabled;
        this.entityMatchEnabled = entityMatchEnabled;
        this.onlyEntityMatchGAEnabled = onlyEntityMatchGAEnabled;
        this.systemAttrs = SchemaRepository //
                .getSystemAttributes(BusinessEntity.Account, entityMatchEnabled).stream() //
                .map(InterfaceName::name).collect(Collectors.toSet());
        this.exportAttrs = SchemaRepository //
                .getDefaultExportAttributes(BusinessEntity.Account, entityMatchEnabled).stream() //
                .map(InterfaceName::name).collect(Collectors.toSet());
        this.attrNameInOtherIDAndMatchID = attrNameInOtherIDAndMatchID;
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

        if (InterfaceName.AccountId.name().equalsIgnoreCase(cm.getAttrName())
                || InterfaceName.CustomerAccountId.name().equalsIgnoreCase(cm.getAttrName())) {
            cm.setSubcategory(Category.SUB_CAT_ACCOUNT_IDS);
        }

        if (systemAttrs.contains(cm.getAttrName())) {
            return cm;
        }

        if (InterfaceName.AccountId.name().equals(cm.getAttrName()) && entityMatchEnabled) {
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
        if (InterfaceName.CustomerAccountId.name().equals(cm.getAttrName()) && entityMatchEnabled) {
            cm.enableGroup(Enrichment);
            cm.disableGroup(TalkingPoint);
            cm.disableGroup(CompanyProfile);
            cm.disableGroup(Model);
            // do not use CustomerAccountId in segment, as there will always be a system id that can be used
            // M36: still enable it. after asking all customer to move away from CustomerAccountId, can disable it.
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

        // DP-12913 Hide other ids for entity match GA
        if (attrNameInOtherIDAndMatchID.contains(cm.getAttrName()) && entityMatchEnabled) {
            if (onlyEntityMatchGAEnabled) {
                cm.disableGroup(Segment);
                cm.disableGroup(Enrichment);
                cm.disableGroup(TalkingPoint);
                cm.disableGroup(CompanyProfile);
                cm.disableGroup(Model);
                cm.setCanSegment(false);
                cm.setCanEnrich(false);
                cm.setCanModel(false);
                return cm;
            } else {
                cm.disableGroup(Segment);
                cm.enableGroup(Enrichment);
                cm.disableGroup(TalkingPoint);
                cm.disableGroup(CompanyProfile);
                cm.disableGroup(Model);
                cm.setCanSegment(true);
                cm.setCanEnrich(true);
                cm.setCanModel(false);
                return cm;
            }
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
