package com.latticeengines.domain.exposed.metadata.mds;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public final class ColumnMetadataUtils {

    // TODO: not completed before review, will keep adding overwriting methods
    static ColumnMetadata overwrite(ColumnMetadata overwriter, ColumnMetadata overwritee) {
        if (!overwriter.getAttrName().equals(overwritee.getAttrName())) {
            throw new IllegalArgumentException(String.format("Cannot use the metadata of %s to overwrite %s",
                    overwriter.getAttrName(), overwritee.getAttrName()));
        }
        if (StringUtils.isNotBlank(overwriter.getDisplayName())) {
            overwritee.setDisplayName(overwriter.getDisplayName());
        }
        if (StringUtils.isNotBlank(overwriter.getDescription())) {
            overwritee.setDescription(overwriter.getDescription());
        }
        if (overwriter.getCategory() != null) {
            overwritee.setCategory(overwriter.getCategory());
        }
        if (StringUtils.isNotBlank(overwriter.getSubcategory())) {
            overwritee.setSubcategory(overwriter.getSubcategory());
        }
        if (overwriter.getEntity() != null) {
            overwritee.setEntity(overwriter.getEntity());
        }
        if (StringUtils.isNotBlank(overwriter.getDataLicense())) {
            overwritee.setDataLicense(overwriter.getDataLicense());
        }
        if (overwriter.getStatisticalType() != null) {
            overwritee.setStatisticalType(overwriter.getStatisticalType());
        }
        if (overwriter.getFundamentalType() != null) {
            overwritee.setFundamentalType(overwriter.getFundamentalType());
        }

        // usage groups
        if (MapUtils.isNotEmpty(overwriter.getGroups())) {
            overwritee.setGroups(overwritingGroups(overwriter.getGroups(), overwritee.getGroups()));
        }

        // for bit encode
        if (StringUtils.isNotBlank(overwriter.getPhysicalName())) {
            overwritee.setPhysicalName(overwriter.getPhysicalName());
        }
        if (overwriter.getBitOffset() != null) {
            overwritee.setBitOffset(overwriter.getBitOffset());
        }
        if (overwriter.getNumBits() != null) {
            overwritee.setNumBits(overwriter.getNumBits());
        }

        // statistics
        if (overwriter.getStats() != null) {
            overwritee.setStats(overwriter.getStats());
        }

        return overwritee;
    }

    private static Map<ColumnSelection.Predefined, Boolean> overwritingGroups(
            Map<ColumnSelection.Predefined, Boolean> overwriter, Map<ColumnSelection.Predefined, Boolean> overwritee) {
        if (MapUtils.isNotEmpty(overwritee)) {
            overwritee.putAll(overwriter);
        } else {
            overwritee = overwriter;
        }
        return overwritee;
    }

}
