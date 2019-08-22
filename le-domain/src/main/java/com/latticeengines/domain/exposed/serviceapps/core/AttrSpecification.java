package com.latticeengines.domain.exposed.serviceapps.core;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class AttrSpecification {

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
    private boolean stateChange;
    private boolean approvedUsageChange;

    private AttrSpecification(String specification, boolean segmentationChange, boolean enrichmentChange,
            boolean companyProfileChange, boolean talkingPointChange, boolean modelChange, boolean typeChange,
            boolean displayNameChange, boolean descriptionChange, boolean categoryNameChange, boolean iconChange,
            boolean stateChange, boolean approvedUsageChange) {
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
        this.stateChange = stateChange;
        this.approvedUsageChange = approvedUsageChange;
    }

    public static AttrSpecification LDC_NON_PREMIUM() {
        return new AttrSpecification("LDC Non-Premium Attributes", true, true, true, true, true, false, false, false,
                true, false, true, true); //
    }

    public static AttrSpecification LDC_PREMIUM() {
        return new AttrSpecification("LDC Premium Attributes", true, true, true, true, true, false, false, false, true,
                false, true, true);
    }

    public static AttrSpecification LDC_INTERNAL() {
        return new AttrSpecification("LDC Internal Attributes", true, true, true, false, true, false, false, false,
                true, false, true, false);
    }

    public static AttrSpecification CDL_STD() {
        return new AttrSpecification("CDL Standard Attributes", true, true, true, true, true, false, true, true, false,
                false, false, false);
    }

    public static AttrSpecification CDL_LOOKUP_ID() {
        return new AttrSpecification("CDL Lookup Ids", true, true, true, true, false, false, true, true, true, false,
                false, false);
    }

    public static AttrSpecification CDL_ACCOUNT_EXTENSION() {
        return new AttrSpecification("CDL Account Extensions", true, true, true, true, true, false, true, true, true,
                false, true, true);
    }

    public static AttrSpecification CDL_CONTACT_EXTENSION() {
        return new AttrSpecification("CDL Contact Extensions", true, true, true, true, false, false, true, true, true,
                false, true, false);
    }

    // Product Spent Profile
    public static AttrSpecification CDL_DERIVED_PB() {
        return new AttrSpecification("CDL Derived Attributes for Product Bundles", //
                true, // segment
                true, // enrich
                true, // company profile
                true, // talking point
                false, // model
                false, // type
                false, // display name
                false, // description
                false, // category
                false, // icon
                false, // state
                false); // approved usage
    }

    // Has Purchased in Product Spent Profile
    public static AttrSpecification CDL_DERIVED_PB_HP() {
        return new AttrSpecification("CDL Derived Attributes for Product Bundles (Has Purchased)", //
                true, // segment
                false, // enrich
                false, // company profile
                false, // talking point
                false, // model
                false, // type
                false, // display name
                false, // description
                false, // category
                false, // icon
                false, // state
                false); // approved usage
    }

    // APS Attributes
    public static AttrSpecification CDL_DERIVED_APS() {
        return new AttrSpecification("CDL Derived Attributes for APS Attributes", //
                false, // segment
                false, // enrich
                false, // company profile
                false, // talking point
                true, // model
                false, // type
                false, // display name
                false, // description
                false, // category
                false, // icon
                false, // state
                false); // approved usage
    }

    public static AttrSpecification CDL_DERIVED_WBC() {
        return new AttrSpecification("CDL Derived Attributes for Website Behavior Categories", true, true, false, true,
                false, false, false, true, false, false, true, false);
    }

    public static AttrSpecification CDL_RATING() {
        return new AttrSpecification("CDL Rating Attributes", true, true, true, true, false, false, false, false, false,
                false, false, false);
    }

    public static AttrSpecification CDL_SEGMENTS() {
        return new AttrSpecification("CDL Segments as Attributes", true, true, true, true, false, false, false, true,
                true, false, true, false);
    }

    public static AttrSpecification CDL_CURATED_ACC() {
        return new AttrSpecification("CDL Derived Attributes for Account", //
                true, // segment
                true, // export
                true, // cp
                true, // tp
                false, // model
                false, // type
                false, // display name
                false, // description
                false, // category
                false, // icon
                false, // state
                false // approved usage
        );
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
                return AttrSpecification.LDC_NON_PREMIUM();
            case Premium:
                return AttrSpecification.LDC_PREMIUM();
            case InternalEnrich:
                return AttrSpecification.LDC_INTERNAL();
            default:
                break;
            }
            break;
        case Custom:
            switch (attrSubType) {
            case Standard:
                return AttrSpecification.CDL_STD();
            case LookupId:
                return AttrSpecification.CDL_LOOKUP_ID();
            case Extension:
                if (entity != null && entity == BusinessEntity.Account) {
                    return AttrSpecification.CDL_ACCOUNT_EXTENSION();
                } else if (entity != null && entity == BusinessEntity.Contact) {
                    return AttrSpecification.CDL_CONTACT_EXTENSION();
                }
            default:
                break;
            }
            break;
        case Curated:
            switch (attrSubType) {
            case ProductBundle:
                return AttrSpecification.CDL_DERIVED_PB();
            case HasPurchased:
                return AttrSpecification.CDL_DERIVED_PB_HP();
            case APS:
                return AttrSpecification.CDL_DERIVED_APS();
            case Rating:
                return AttrSpecification.CDL_RATING();
            case CuratedAccount:
                return AttrSpecification.CDL_CURATED_ACC();
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

    public boolean stateChange() {
        return stateChange;
    }

    public boolean approvedUsageChange() {
        return approvedUsageChange;
    }

    public void disableStateChange() {
        stateChange = false;
    }

    public void setSegmentationChange(boolean segmentationChange) {
        this.segmentationChange = segmentationChange;
    }

    public void setEnrichmentChange(boolean enrichmentChange) {
        this.enrichmentChange = enrichmentChange;
    }

    public void setCompanyProfileChange(boolean companyProfileChange) {
        this.companyProfileChange = companyProfileChange;
    }

    public void setTalkingPointChange(boolean talkingPointChange) {
        this.talkingPointChange = talkingPointChange;
    }

    public void setModelChange(boolean modelChange) {
        this.modelChange = modelChange;
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
