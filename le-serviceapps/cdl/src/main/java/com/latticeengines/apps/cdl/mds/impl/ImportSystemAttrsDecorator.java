package com.latticeengines.apps.cdl.mds.impl;

import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.CompanyProfile;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Enrichment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Model;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.Segment;
import static com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined.TalkingPoint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class ImportSystemAttrsDecorator implements Decorator {

    private final Map<String, Pair<S3ImportSystem, EntityType>> accountSystemIdMap;
    private final Map<String, Pair<S3ImportSystem, EntityType>> contactSystemIdMap;
    private final boolean onlyEntityMatchGAEnabled;

    ImportSystemAttrsDecorator(List<S3ImportSystem> s3ImportSystems, boolean onlyEntityMatchGAEnabled) {
        this.accountSystemIdMap = new HashMap<>();
        this.contactSystemIdMap = new HashMap<>();
        this.onlyEntityMatchGAEnabled = onlyEntityMatchGAEnabled;
        if (CollectionUtils.isNotEmpty(s3ImportSystems)) {
            s3ImportSystems.forEach(s3ImportSystem -> {
                this.accountSystemIdMap.put(s3ImportSystem.getAccountSystemId(), Pair.of(s3ImportSystem,
                        s3ImportSystem.getSystemType().getPrimaryAccount()));
                if (s3ImportSystem.getSecondaryAccountIds() != null) {
                    for (Map.Entry<String, EntityType> entry :
                            s3ImportSystem.getSecondaryAccountIds().getSecondaryIdToEntityTypeMap().entrySet()) {
                        this.accountSystemIdMap.put(entry.getKey(), Pair.of(s3ImportSystem, entry.getValue()));
                    }
                }
                this.contactSystemIdMap.put(s3ImportSystem.getContactSystemId(), Pair.of(s3ImportSystem,
                        s3ImportSystem.getSystemType().getPrimaryContact()));
                if (s3ImportSystem.getSecondaryContactIds() != null) {
                    for (Map.Entry<String, EntityType> entry :
                            s3ImportSystem.getSecondaryContactIds().getSecondaryIdToEntityTypeMap().entrySet()) {
                        this.contactSystemIdMap.put(entry.getKey(), Pair.of(s3ImportSystem, entry.getValue()));
                    }
                }
            });
        }
    }

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        return metadata.map(this::process);
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        return metadata.map(this::process);
    }

    @Override
    public String getName() {
        return "systemid-attrs";
    }

    private ColumnMetadata process(ColumnMetadata cm) {
        if (isAccountSystemId(cm) || isAccountSystemIdInContactTemplate(cm)) {
            return processAccountSystemId(cm);
        } else if (isContactSystemId(cm)) {
            return processContactSystemId(cm);
        } else {
            return cm;
        }
    }

    private ColumnMetadata processAccountSystemId(ColumnMetadata cm) {
        Pair<S3ImportSystem, EntityType> attrInfoPair = accountSystemIdMap.get(cm.getAttrName());
        cm.setDisplayName(String.format("%s %s ID", attrInfoPair.getLeft().getName(),
                convertPluralToSingular(attrInfoPair.getRight().getDisplayName())));
        cm.setSubcategory(Category.SUB_CAT_ACCOUNT_IDS);
        configEntityMatch(cm);
        return cm;
    }

    // the original display name is the entity type's display name
    // which is usually plural
    // for now, removing the trailing s can convert them to singular
    private String convertPluralToSingular(String displayName) {
        if (StringUtils.isNotEmpty(displayName) && displayName.endsWith("s")) {
            return displayName.substring(0, displayName.length() - 1);
        }
        return displayName;
    }

    private ColumnMetadata processContactSystemId(ColumnMetadata cm) {
        Pair<S3ImportSystem, EntityType> attrInfoPair = contactSystemIdMap.get(cm.getAttrName());
        cm.setDisplayName(String.format("%s %s ID", attrInfoPair.getLeft().getName(),
                convertPluralToSingular(attrInfoPair.getRight().getDisplayName())));
        configEntityMatch(cm);
        return cm;
    }

    // PLS-15406 setting for attributes corresponds to mappings in section
    // Unique ID, Other IDs, Match IDs, only enable for usage export
    // DP-12913 Hide other ids for entity match GA
    private void configEntityMatch(ColumnMetadata cm) {
        if (onlyEntityMatchGAEnabled) {
            cm.disableGroup(Segment);
            cm.disableGroup(Enrichment);
            cm.disableGroup(TalkingPoint);
            cm.disableGroup(CompanyProfile);
            cm.disableGroup(Model);
            cm.setCanSegment(false);
            cm.setCanEnrich(false);
            cm.setCanModel(false);
        } else {
            cm.disableGroup(Segment);
            cm.enableGroup(Enrichment);
            cm.disableGroup(TalkingPoint);
            cm.disableGroup(CompanyProfile);
            cm.disableGroup(Model);
            cm.setCanSegment(true);
            cm.setCanEnrich(true);
            cm.setCanModel(false);
        }
    }

    private boolean isAccountSystemId(ColumnMetadata cm) {
        return BusinessEntity.Account.equals(cm.getEntity()) &&  accountSystemIdMap.containsKey(cm.getAttrName());
    }

    private boolean isContactSystemId(ColumnMetadata cm) {
        return BusinessEntity.Contact.equals(cm.getEntity()) && contactSystemIdMap.containsKey(cm.getAttrName());
    }

    private boolean isAccountSystemIdInContactTemplate(ColumnMetadata cm) {
        return BusinessEntity.Contact.equals(cm.getEntity()) && accountSystemIdMap.containsKey(cm.getAttrName());
    }
}
