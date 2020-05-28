package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public final class CategoryUtils {

    protected CategoryUtils() {
        throw new UnsupportedOperationException();
    }

    public static List<BusinessEntity> getEntity(Category category) {
        switch (category) {
            case CONTACT_ATTRIBUTES:
                return Collections.singletonList(BusinessEntity.Contact);
            case PRODUCT_SPEND:
                return Arrays.asList(BusinessEntity.PurchaseHistory, BusinessEntity.AnalyticPurchaseState);
            case RATING:
                return Collections.singletonList(BusinessEntity.Rating);
            case CURATED_ACCOUNT_ATTRIBUTES:
                return Collections.singletonList(BusinessEntity.CuratedAccount);
            case CURATED_CONTACT_ATTRIBUTES:
                return Collections.singletonList(BusinessEntity.CuratedContact);
            case WEB_VISIT_PROFILE:
                return Collections.singletonList(BusinessEntity.WebVisitProfile);
            case OPPORTUNITY_PROFILE:
                return Collections.singletonList(BusinessEntity.Opportunity);
            case ACCOUNT_MARKETING_ACTIVITY_PROFILE:
                return Collections.singletonList(BusinessEntity.AccountMarketingActivity);
            case CONTACT_MARKETING_ACTIVITY_PROFILE:
                return Collections.singletonList(BusinessEntity.ContactMarketingActivity);
            case DNBINTENTDATA_PROFILE:
                return Collections.singletonList(BusinessEntity.CustomIntent);
            default:
                return Collections.singletonList(BusinessEntity.Account);
        }
    }

    public static Category getEntityCategory(BusinessEntity entity) {
        Category category;
        switch (entity) {
            case Account:
                category = Category.ACCOUNT_ATTRIBUTES;
                break;
            case Contact:
                category = Category.CONTACT_ATTRIBUTES;
                break;
            case PurchaseHistory:
            case AnalyticPurchaseState:
                category = Category.PRODUCT_SPEND;
                break;
            case Rating:
                category = Category.RATING;
                break;
            case CuratedAccount:
                category = Category.CURATED_ACCOUNT_ATTRIBUTES;
                break;
            case CuratedContact:
                category = Category.CURATED_CONTACT_ATTRIBUTES;
                break;
            case WebVisitProfile:
                category = Category.WEB_VISIT_PROFILE;
                break;
            case Opportunity:
                category = Category.OPPORTUNITY_PROFILE;
                break;
            case AccountMarketingActivity:
                category = Category.ACCOUNT_MARKETING_ACTIVITY_PROFILE;
                break;
            case ContactMarketingActivity:
                category = Category.CONTACT_MARKETING_ACTIVITY_PROFILE;
                break;
            case CustomIntent:
                category = Category.DNBINTENTDATA_PROFILE;
            default:
                category = Category.DEFAULT;
        }
        return category;
    }

}
