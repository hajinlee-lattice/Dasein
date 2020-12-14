package com.latticeengines.domain.exposed.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public final class ExportUtils {

    private static final Logger log = LoggerFactory.getLogger(ExportUtils.class);

    public static final String CONTACT_ATTR_PREFIX = "ContactRenamed_";

    public static final String EMPTY_CATEGORY = "emptyCategory";

    public static final String replacedByUnderscore = "[<>:\"/\\\\|?*]";

    protected ExportUtils() {
        throw new UnsupportedOperationException();
    }

    private static void renameDisplayNameMap(ExportEntity exportEntity, ColumnMetadata cm, String displayName, Map<String, String> displayNameMap) {
        // need to rename contact column name
        if (ExportEntity.AccountContact.equals(exportEntity) && //
                BusinessEntity.Contact.equals(BusinessEntity.getCentralEntity(cm.getEntity()))) {
            displayNameMap.put(CONTACT_ATTR_PREFIX + cm.getAttrName(), displayName);
        } else {
            displayNameMap.put(cm.getAttrName(), displayName);
        }
    }

    private static class DisplayData {

        private ColumnMetadata columnMetadata;
        private boolean displayNameUpdated;

        private DisplayData(ColumnMetadata columnMetadata, boolean displayNameUpdated) {
            this.columnMetadata = columnMetadata;
            this.displayNameUpdated = displayNameUpdated;
        }

        public boolean isDisplayNameUpdated() {
            return displayNameUpdated;
        }

        public void setDisplayNameUpdated(boolean displayNameUpdated) {
            this.displayNameUpdated = displayNameUpdated;
        }

        public ColumnMetadata getColumnMetadata() {
            return columnMetadata;
        }
    }

    private static void insertDataIntoDisplayNameMap(ColumnMetadata cm, ExportEntity exportEntity, Map<String, DisplayData> outputCols,
                                                     Map<String, String> displayNameMap, String originalDisplayName, int indexToAppend) {
        boolean putDisplayName = true;
        String displayName = originalDisplayName;
        DisplayData displayData = outputCols.get(originalDisplayName.toLowerCase());
        if (displayData != null) {
            putDisplayName = false;
            Category category = cm.getCategory();
            if (indexToAppend > 1) {
                // display name may duplicated in same category, if so append index to display name
                displayName = category == null ? originalDisplayName + "(" + indexToAppend + ")" :
                        category.getName() + "_" + originalDisplayName + "(" + indexToAppend + ")";
            } else {
                displayName = category == null ? originalDisplayName : category.getName() + "_" + originalDisplayName;
            }
            log.warn(String.format("Display name [%s] has already been assigned to another attr, cannot be " +
                    "assigned to [%s]. Display name changed to [%s].", originalDisplayName, cm.getAttrName(), displayName));
            if (!displayData.isDisplayNameUpdated()) {
                displayData.setDisplayNameUpdated(true);
                ColumnMetadata columnMetadata = displayData.getColumnMetadata();
                category = columnMetadata.getCategory();
                if (category != null) {
                    renameDisplayNameMap(exportEntity, columnMetadata,
                            category.getName() + "_" + columnMetadata.getDisplayName(), displayNameMap);
                }
            }
        }
        renameDisplayNameMap(exportEntity, cm, displayName, displayNameMap);
        if (putDisplayName) {
            outputCols.put(displayName.toLowerCase(), new DisplayData(cm, false));
        }
    }

    public static Map<String, String> getDisplayNameMap(ExportEntity exportEntity, List<ColumnMetadata> schema) {
        Map<String, String> displayNameMap = new HashMap<>();
        Map<String, DisplayData> outputCols = new HashMap<>();
        Map<String, Map<String, MutableInt>> displayNameIndexMap = new HashMap<>();
        schema.forEach(cm -> {
            String originalDisplayName = cm.getDisplayName();
            String originalDisplayNameLowerCase = originalDisplayName.toLowerCase();
            String categoryName = cm.getCategory() == null ? EMPTY_CATEGORY : cm.getCategory().name();
            Map<String, MutableInt> indexMap = displayNameIndexMap.get(categoryName);
            int indexToAppend = 1;
            if (MapUtils.isNotEmpty(indexMap)) {
                MutableInt index = indexMap.get(originalDisplayNameLowerCase);
                if (index != null) {
                    index.increment();
                    indexToAppend = index.getValue();
                } else {
                    indexMap.put(originalDisplayNameLowerCase, new MutableInt(indexToAppend));
                }
            } else {
                indexMap = new HashMap<>();
                indexMap.put(originalDisplayNameLowerCase, new MutableInt(indexToAppend));
                displayNameIndexMap.put(categoryName, indexMap);
            }
            insertDataIntoDisplayNameMap(cm, exportEntity, outputCols, displayNameMap, originalDisplayName, indexToAppend);
        });
        return displayNameMap;
    }

    public static String getReplacedName(String name) {
        if (StringUtils.isNotEmpty(name)) {
            return name.replaceAll(replacedByUnderscore, "_");
        } else {
            return name;
        }
    }
}
