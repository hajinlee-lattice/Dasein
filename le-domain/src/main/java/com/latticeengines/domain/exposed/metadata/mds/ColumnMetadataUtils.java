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
            throw new IllegalArgumentException(
                    String.format("Cannot use the metadata of %s to overwrite %s",
                            overwriter.getAttrName(), overwritee.getAttrName()));
        }
        if (StringUtils.isNotBlank(overwriter.getJavaClass())) {
            overwritee.setJavaClass(overwriter.getJavaClass());
        }
        if (StringUtils.isNotBlank(overwriter.getDisplayName())) {
            overwritee.setDisplayName(overwriter.getDisplayName());
        }
        if (StringUtils.isNotBlank(overwriter.getSecondaryDisplayName())) {
            overwritee.setSecondaryDisplayName(overwriter.getSecondaryDisplayName());
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
        if (StringUtils.isNotBlank(overwriter.getLastDataRefresh())) {
            overwritee.setLastDataRefresh(overwriter.getLastDataRefresh());
        }
        if (overwriter.getStatisticalType() != null) {
            overwritee.setStatisticalType(overwriter.getStatisticalType());
        }
        if (overwriter.getFundamentalType() != null) {
            overwritee.setFundamentalType(overwriter.getFundamentalType());
        }
        if (overwriter.getEntity() != null) {
            overwritee.setEntity(overwriter.getEntity());
        }

        if (overwriter.getAttrState() != null) {
            overwritee.setAttrState(overwriter.getAttrState());
        }

        if (overwriter.getShouldDeprecate() != null) {
            overwritee.setShouldDeprecate(overwriter.getShouldDeprecate());
        }

        // can change flags
        if (overwriter.getCanSegment() != null) {
            overwritee.setCanSegment(overwriter.getCanSegment());
        }
        if (overwriter.getCanEnrich() != null) {
            overwritee.setCanEnrich(overwriter.getCanEnrich());
        }
        if (overwriter.getCanInternalEnrich() != null) {
            overwritee.setCanInternalEnrich(overwriter.getCanInternalEnrich());
        }
        if (overwriter.getCanModel() != null) {
            overwritee.setCanModel(overwriter.getCanModel());
        }

        // usage groups
        if (overwriter.getShouldDeprecate() != null) {
            overwritee.setShouldDeprecate(overwriter.getShouldDeprecate());
        }
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
            Map<ColumnSelection.Predefined, Boolean> overwriter,
            Map<ColumnSelection.Predefined, Boolean> overwritee) {
        if (MapUtils.isNotEmpty(overwritee)) {
            overwritee.putAll(overwriter);
        } else {
            overwritee = overwriter;
        }
        return overwritee;
    }

}
