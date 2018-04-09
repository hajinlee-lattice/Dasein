package com.latticeengines.domain.exposed.util;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public final class CategoryUtils {

    public static BusinessEntity getEntity(Category category) {
        BusinessEntity entity;
        switch (category) {
            case CONTACT_ATTRIBUTES:
                entity = BusinessEntity.Contact;
                break;
            case PRODUCT_SPEND:
                entity = BusinessEntity.PurchaseHistory;
                break;
            case RATING:
                entity = BusinessEntity.Rating;
                break;
            default:
                entity = BusinessEntity.Account;
        }
        return entity;
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
                category = Category.PRODUCT_SPEND;
                break;
            case Rating:
                category = Category.RATING;
                break;
            default:
                category = Category.DEFAULT;
        }
        return category;
    }

}
