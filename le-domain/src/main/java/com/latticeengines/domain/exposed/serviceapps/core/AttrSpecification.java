package com.latticeengines.domain.exposed.serviceapps.core;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public enum AttrSpecification {

    LDC_NON_PREMIUM("LDC Non-Premium Attributes", true, true, true, true, true, false, false, false, true, false),
    LDC_PREMIUM("LDC Premium Attributes", true, true, true, true, true, false, false, false, true, false),
    LDC_INTERNAL("LDC Internal Attributes", false, true, false, true, true, false, false, false, true, false),
    CDL_STD("CDL Standard Attributes", true, true, true, true, true, false, false, false, false, false),
    CDL_LOOKUP_ID("CDL Lookup Ids", true, true, true, true, false, false, false, true, true, false),
    CDL_ACCOUNT_EXTENSION("CDL Account Extensions", true, true, true, true, true, false, false, true, true, false),
    CDL_CONTACT_EXTENSION("CDL Contact Extensions", true, true, true, true, true, false, false, true, true, false),
    CDL_DERIVED_PB("CDL Derived Attributes for Product Bundles", true, false, false, false, false, false, false, false,
            false, false),
    CDL_DERIVED_WBC("CDL Derived Attributes for Website Behavior Categories", true, true, true, true, false, false, false, true, false, false),
    CDL_RATING("CDL Rating Attributes", true, false, false, true, false, false, false, false, false,
            false),
    CDL_SEGMENTS("CDL Segments as Attributes", true, true, true, true, false, false, true, true, true, false);

    private String specification;
    private boolean segmentationChange;
    private boolean enrichmentChange;
    private boolean companyProfileChange;
    private boolean talkingPointChange;
    private boolean modelChange;
    private boolean typeChange;
    private boolean displayNameChange;
    private boolean descriptionChange;
    private boolean categoryNameChange;
    private boolean iconChange;

    AttrSpecification(String specification, boolean segmentationChange, boolean enrichmentChange,
                      boolean companyProfileChange, boolean talkingPointChange, boolean modelChange,
                      boolean typeChange, boolean displayNameChange, boolean descriptionChange,
                      boolean categoryNameChange, boolean iconChange) {
        this.specification = specification;
        this.segmentationChange = segmentationChange;
        this.enrichmentChange = enrichmentChange;
        this.companyProfileChange = companyProfileChange;
        this.talkingPointChange = talkingPointChange;
        this.modelChange = modelChange;
        this.typeChange = typeChange;
        this.displayNameChange = displayNameChange;
        this.descriptionChange = descriptionChange;
        this.categoryNameChange = categoryNameChange;
        this.iconChange = iconChange;
    }

    public static AttrSpecification getAttrSpecification(AttrType attrType, AttrSubType attrSubType,
                                                          BusinessEntity entity) {
        if (attrType == null) {
            throw new IllegalArgumentException("AttrType cannot be null!");
        }
        if (attrSubType == null) {
            throw new IllegalArgumentException("AttrSubType cannot be null!");
        }
        switch (attrType) {
            case DataCloud:
                switch (attrSubType) {
                    case Normal:
                        return AttrSpecification.LDC_NON_PREMIUM;
                    case Premium:
                        return AttrSpecification.LDC_PREMIUM;
                    case InternalEnrich:
                        return AttrSpecification.LDC_INTERNAL;
                    default:
                        break;
                }
                break;
            case Custom:
                switch (attrSubType) {
                    case Standard:
                        return AttrSpecification.CDL_STD;
                    case LookupId:
                        return AttrSpecification.CDL_LOOKUP_ID;
                    case Extension:
                        if (entity != null && entity == BusinessEntity.Account) {
                            return AttrSpecification.CDL_ACCOUNT_EXTENSION;
                        } else if (entity != null && entity == BusinessEntity.Contact) {
                            return AttrSpecification.CDL_CONTACT_EXTENSION;
                        }
                    default:
                        break;
                }
                break;
            case Curated:
                switch (attrSubType) {
                    case ProductBundle:
                        return AttrSpecification.CDL_DERIVED_PB;
                    case Rating:
                        return AttrSpecification.CDL_RATING;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return null;
    }

    public String getSpecification() {
        return specification;
    }

    public boolean segmentationChange() {
        return segmentationChange;
    }

    public boolean enrichmentChange() {
        return enrichmentChange;
    }

    public boolean companyProfileChange() {
        return companyProfileChange;
    }

    public boolean talkingPointChange() {
        return talkingPointChange;
    }

    public boolean modelChange() {
        return modelChange;
    }

    public boolean typeChange() {
        return typeChange;
    }

    public boolean displayNameChange() {
        return displayNameChange;
    }

    public boolean descriptionChange() {
        return descriptionChange;
    }

    public boolean categoryNameChange() {
        return categoryNameChange;
    }

    public boolean iconChange() {
        return iconChange;
    }

    public boolean allowChange(ColumnSelection.Predefined group) {
        if (group == null) {
            return true;
        }
        switch (group) {
            case Segment:
                return segmentationChange;
            case Enrichment:
                return enrichmentChange;
            case CompanyProfile:
                return companyProfileChange;
            case TalkingPoint:
                return talkingPointChange;
            case Model:
                return modelChange;
            default:
                return true;
        }
    }
}
