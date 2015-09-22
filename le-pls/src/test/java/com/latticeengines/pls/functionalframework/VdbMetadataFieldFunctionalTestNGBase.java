package com.latticeengines.pls.functionalframework;

import java.util.List;
import java.util.Map;

import org.junit.Assert;

import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.pls.service.VdbMetadataConstants;

public class VdbMetadataFieldFunctionalTestNGBase extends PlsFunctionalTestNGBase {

    protected VdbMetadataField getField(List<VdbMetadataField> fields, String columnName) {
        for (VdbMetadataField field : fields) {
            if (columnName.equals(field.getColumnName())) {
                return field;
            }
        }

        return null;
    }

    protected void assertFieldExists(Map<String, Map<String, String>> colsMetadata, VdbMetadataField field) {
        Assert.assertTrue(colsMetadata.containsKey(field.getColumnName()));
        Map<String, String> map = colsMetadata.get(field.getColumnName());
        Assert.assertEquals(field.getSource(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_SOURCE));
        Assert.assertEquals(field.getCategory(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_CATEGORY));
        Assert.assertEquals(field.getDisplayName(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_DISPLAYNAME));
        Assert.assertEquals(field.getDescription(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_DESCRIPTION));
        Assert.assertEquals(field.getApprovedUsage(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_APPROVED_USAGE));
        Assert.assertEquals(field.getTags(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_TAGS));
        Assert.assertEquals(field.getFundamentalType(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_FUNDAMENTAL_TYPE));
        Assert.assertEquals(field.getDisplayDiscretization(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_DISPLAY_DISCRETIZATION));
        Assert.assertEquals(field.getStatisticalType(), getFieldValue(map, VdbMetadataConstants.ATTRIBUTE_STATISTICAL_TYPE));
    }

    protected String getFieldValue(Map<String, String> map, String key) {
        if (map.containsKey(key)) {
            String value = map.get(key);
            if (VdbMetadataConstants.ATTRIBUTE_FUNDAMENTAL_TYPE.equals(key) &&
                    VdbMetadataConstants.ATTRIBUTE_FUNDAMENTAL_UNKNOWN_VALUE.equalsIgnoreCase(value)) {
                return null;
            } else if (VdbMetadataConstants.ATTRIBUTE_NULL_VALUE.equalsIgnoreCase(value)) {
                return null;
            } else {
                return value;
            }
        } else {
            return null;
        }
    }

}
