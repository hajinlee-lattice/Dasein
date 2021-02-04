package com.latticeengines.domain.exposed.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;

public final class DataCollectionStatusUtils {

    protected DataCollectionStatusUtils() {
        throw new UnsupportedOperationException();
    }
    public static DataCollectionStatus initDateMap(DataCollectionStatus status, Long timestamp) {
        Map<String, Long> dateMap = new HashMap<>();
        dateMap.put(Category.FIRMOGRAPHICS.getName(), timestamp);
        dateMap.put(Category.GROWTH_TRENDS.getName(), timestamp);
        dateMap.put(Category.COVID_19.getName(), timestamp);
        dateMap.put(Category.INTENT.getName(), timestamp);
        dateMap.put(Category.ONLINE_PRESENCE.getName(), timestamp);
        dateMap.put(Category.TECHNOLOGY_PROFILE.getName(), timestamp);
        dateMap.put(Category.DNB_TECHNOLOGY_PROFILE.getName(), timestamp);
        dateMap.put(Category.WEBSITE_KEYWORDS.getName(), timestamp);
        dateMap.put(Category.WEBSITE_PROFILE.getName(), timestamp);
        dateMap.put(Category.ACCOUNT_ATTRIBUTES.getName(), timestamp);
        dateMap.put(Category.CONTACT_ATTRIBUTES.getName(), timestamp);
        dateMap.put(Category.CURATED_ACCOUNT_ATTRIBUTES.getName(), timestamp);
        dateMap.put(Category.RATING.getName(), timestamp);
        dateMap.put(Category.PRODUCT_SPEND.getName(), timestamp);
        status.setDateMap(dateMap);
        return status;
    }

    public static DataCollectionStatus updateTimeForDCChange(DataCollectionStatus status,
            Long timestamp) {
        Map<String, Long> dateMap = status.getDateMap();
        if (MapUtils.isEmpty(dateMap)) {
            status = initDateMap(status, timestamp);
            dateMap = status.getDateMap();
        }
        dateMap.put(Category.FIRMOGRAPHICS.getName(), timestamp);
        dateMap.put(Category.GROWTH_TRENDS.getName(), timestamp);
        dateMap.put(Category.COVID_19.getName(), timestamp);
        dateMap.put(Category.INTENT.getName(), timestamp);
        dateMap.put(Category.ONLINE_PRESENCE.getName(), timestamp);
        dateMap.put(Category.TECHNOLOGY_PROFILE.getName(), timestamp);
        dateMap.put(Category.DNB_TECHNOLOGY_PROFILE.getName(), timestamp);
        dateMap.put(Category.WEBSITE_KEYWORDS.getName(), timestamp);
        dateMap.put(Category.WEBSITE_PROFILE.getName(), timestamp);
        return status;
    }

    public static DataCollectionStatus updateTimeForCategoryChange(DataCollectionStatus status,
            Long timestamp, Category category) {
        Map<String, Long> dateMap = status.getDateMap();
        if (MapUtils.isEmpty(dateMap)) {
            dateMap = new HashMap<>();
            status.setDateMap(dateMap);
        }
        dateMap.put(category.getName(), timestamp);
        return status;
    }

}
