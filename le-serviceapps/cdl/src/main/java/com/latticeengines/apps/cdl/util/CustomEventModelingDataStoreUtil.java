package com.latticeengines.apps.cdl.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;

public final class CustomEventModelingDataStoreUtil {

    protected CustomEventModelingDataStoreUtil() {
        throw new UnsupportedOperationException();
    }

    public static Set<Category> getCategoriesByDataStores(List<CustomEventModelingConfig.DataStore> dataStores) {
        Set<Category> selectedCategories = new HashSet<>();
        if (CollectionUtils.isEmpty(dataStores)) {
            return selectedCategories;
        }
        if (dataStores.contains(CustomEventModelingConfig.DataStore.DataCloud)) {
            selectedCategories.addAll(Category.getLdcReservedCategories());
        }
        if (dataStores.contains(CustomEventModelingConfig.DataStore.CDL)) {
            selectedCategories.add(Category.ACCOUNT_ATTRIBUTES);
        }
        return selectedCategories;
    }
}
