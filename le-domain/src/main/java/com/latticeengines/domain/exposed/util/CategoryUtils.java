package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public final class CategoryUtils {

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
        case WEB_VISIT_PROFILE:
            return Collections.singletonList(BusinessEntity.WebVisitProfile);
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
        case WebVisitProfile:
            category = Category.WEB_VISIT_PROFILE;
            break;
        default:
            category = Category.DEFAULT;
        }
        return category;
    }

}
