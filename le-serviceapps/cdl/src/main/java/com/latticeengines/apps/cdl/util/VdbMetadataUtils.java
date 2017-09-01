package com.latticeengines.apps.cdl.util;


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;

public class VdbMetadataUtils {

    private static final Logger log = LoggerFactory.getLogger(VdbMetadataUtils.class);

    static final Set<String> unparsableFundamentalTypes = new HashSet<>(Arrays.asList( //
            "alert", "segment", "unknown"
    ));

    public static Attribute convertToAttribute(VdbSpecMetadata metadata) {
        try {
            Attribute attr = new Attribute();
            attr.setNullable(Boolean.TRUE);
            attr.setName(AvroUtils.getAvroFriendlyString(metadata.getColumnName()));
            attr.setSourceAttrName(metadata.getColumnName());
            attr.setDisplayName(metadata.getDisplayName());
            attr.setSourceLogicalDataType(metadata.getDataType());
            attr.setPhysicalDataType(metadata.getDataType());
            attr.setDescription(metadata.getDescription());
            attr.setDataSource(metadata.getDataSource());
            attr.setFundamentalType(resolveFundamentalType(metadata));
            attr.setStatisticalType(resolveStatisticalType(metadata));
            attr.setTags(metadata.getTags());
            attr.setApprovedUsage(metadata.getApprovedUsage());
            attr.setDisplayDiscretizationStrategy(metadata.getDisplayDiscretizationStrategy());
            if (metadata.getDataQuality() != null && metadata.getDataQuality().size() > 0) {
                attr.setDataQuality(metadata.getDataQuality().get(0));
            }
            return attr;
        } catch (Exception e) {
            // see the log to add unit test
            throw new RuntimeException(String.format("Failed to parse vdb metadata %s", JsonUtils.serialize(metadata)), e);
        }
    }

    public static boolean isAcceptableDataType(String srcType, String destType) {
        if (StringUtils.isEmpty(srcType) || StringUtils.isEmpty(destType)) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(srcType, destType)) {
            if (srcType.equalsIgnoreCase("int")) {
                if (!destType.equalsIgnoreCase("long")) {
                    return false;
                }
            } else if (srcType.equalsIgnoreCase("long")) {
                if (!destType.equalsIgnoreCase("int")) {
                    return false;
                }
            } else if (srcType.equalsIgnoreCase("float")) {
                if (!destType.equalsIgnoreCase("double")) {
                    return false;
                }
            } else if (srcType.equalsIgnoreCase("double")) {
                if (!destType.equalsIgnoreCase("float")) {
                    return false;
                }
            } else if (srcType.equalsIgnoreCase("string")) {
                if (!destType.toLowerCase().startsWith("varchar") && !destType.toLowerCase().startsWith("nvarchar")) {
                    return false;
                }
            } else if (srcType.toLowerCase().startsWith("varchar")) {
                if (!destType.equalsIgnoreCase("string") && !destType.toLowerCase().startsWith("nvarchar")) {
                    return false;
                }
            } else if (srcType.toLowerCase().startsWith("nvarchar")) {
                if (!destType.equalsIgnoreCase("string") && !destType.toLowerCase().startsWith("varchar")) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    private static FundamentalType resolveFundamentalType(VdbSpecMetadata metadata) {
        String vdbFundamentalType = metadata.getFundamentalType();
        if (StringUtils.isBlank(vdbFundamentalType)) {
            return null;
        }

        if ("Bit".equalsIgnoreCase(vdbFundamentalType)) {
            vdbFundamentalType = "boolean";
        } else if ("EpochTime".equalsIgnoreCase(vdbFundamentalType)) {
            vdbFundamentalType = "date";
        } else if (unparsableFundamentalTypes.contains(vdbFundamentalType.toLowerCase())) {
            return null;
        }

        try {
            return FundamentalType.fromName(vdbFundamentalType);
        } catch (Exception e) {
            log.warn("Found unknown VDB fundamental type: FundamentalType=" + vdbFundamentalType);
            return null;
        }
    }

    private static StatisticalType resolveStatisticalType(VdbSpecMetadata metadata) {
        String vdbStatisticalType = metadata.getStatisticalType();
        if (StringUtils.isBlank(vdbStatisticalType)) {
            return null;
        }
        try {
            return StatisticalType.fromName(vdbStatisticalType);
        } catch (IllegalArgumentException e) {
            if (metadata.getApprovedUsage() != null &&
                    metadata.getApprovedUsage().contains(ApprovedUsage.NONE.getName())) {
                return null;
            } else {
                metadata.setApprovedUsage(Arrays.asList(ApprovedUsage.NONE.getName()));
                log.error("Found unknown VDB statistical type: StatisticalType=" + vdbStatisticalType);
                return null;
            }
        }
    }

}
