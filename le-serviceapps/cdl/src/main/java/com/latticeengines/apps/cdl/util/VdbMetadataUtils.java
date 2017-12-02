package com.latticeengines.apps.cdl.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbMetadataExtension;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class VdbMetadataUtils {

    private static final Logger log = LoggerFactory.getLogger(VdbMetadataUtils.class);

    static final Set<String> unparsableFundamentalTypes = new HashSet<>(Arrays.asList( //
            "alert", "segment", "unknown"));

    public static Attribute convertToAttribute(VdbSpecMetadata metadata, String entity) {
        try {
            Attribute attr = new Attribute();
            attr.setNullable(Boolean.TRUE);
            attr.setName(AvroUtils.getAvroFriendlyString(metadata.getColumnName()).toLowerCase());
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
            if (attr.getSourceLogicalDataType() != null
                    && Sets.newHashSet("DATE", "DATETIME").contains(attr.getSourceLogicalDataType().toUpperCase())) {
                attr.setLogicalDataType(LogicalDataType.Date);
            } else if (attr.getSourceLogicalDataType() != null
                    && Sets.newHashSet("TIME", "TIMESTAMP").contains(attr.getSourceLogicalDataType().toUpperCase())) {
                attr.setLogicalDataType(LogicalDataType.Timestamp);
            }
            setAttributeExtensions(attr, metadata.getExtensions());
            if (BusinessEntity.getByName(entity) == BusinessEntity.Account) {
                setAttributeGroup(attr);
            }
            return attr;
        } catch (Exception e) {
            // see the log to add unit test
            throw new RuntimeException(String.format("Failed to parse vdb metadata %s", JsonUtils.serialize(metadata)),
                    e);
        }
    }

    private static void setAttributeExtensions(Attribute attribute, List<VdbMetadataExtension> vdbMetadataExtensions) {
        if (vdbMetadataExtensions == null) {
            return;
        }
        for (VdbMetadataExtension extension : vdbMetadataExtensions) {
            if (extension.getKey().equalsIgnoreCase("ExcludeFromFiltering")) {
                attribute.setExcludeFromFiltering(extension.getValue());
            } else if (extension.getKey().equalsIgnoreCase("ExcludeFromPlaymakerExport")) {
                attribute.setExcludeFromPlaymakerExport(extension.getValue());
            } else if (extension.getKey().equalsIgnoreCase("ExcludeFromTalkingPoints")) {
                attribute.setExcludeFromTalkingPoints(extension.getValue());
            } else if (extension.getKey().equalsIgnoreCase("ExcludeFromListView")) {
                attribute.setExcludeFromListView(extension.getValue());
            } else if (extension.getKey().equalsIgnoreCase("ExcludeFromDetailView")) {
                attribute.setExcludeFromDetailView(extension.getValue());
            } else if (extension.getKey().equalsIgnoreCase("ExcludeFromAll")) {
                attribute.setExcludeFromAll(extension.getValue());
            }
        }
    }

    private static void setAttributeGroup(Attribute attribute) {
        List<ColumnSelection.Predefined> groups = new ArrayList<>();
        if (!attribute.getExcludeFromTalkingPoints()) {
            groups.add(ColumnSelection.Predefined.TalkingPoint);
        }
        if (!attribute.getExcludeFromDetailView()) {
            groups.add(ColumnSelection.Predefined.CompanyProfile);
        }
        attribute.setGroupsViaList(groups);
    }

    public static boolean validateVdbTable(Table vdbTable) {
        boolean valid = true;
        if (vdbTable == null) {
            valid = false;
        } else {
            Set<String> attrNames = new HashSet<>();
            for (Attribute attribute : vdbTable.getAttributes()) {
                if (attrNames.contains(attribute.getName().toLowerCase())) {
                    log.error(String.format("Table already have attribute with same name %s (case insensitive)",
                            attribute.getName()));
                    valid = false;
                    break;
                } else {
                    attrNames.add(attribute.getName().toLowerCase());
                }
            }
        }
        return valid;
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
                if (!destType.equalsIgnoreCase("string") && !destType.toLowerCase().startsWith("nvarchar")
                        && !destType.toLowerCase().startsWith("varchar")) {
                    return false;
                }
            } else if (srcType.toLowerCase().startsWith("nvarchar")) {
                if (!destType.equalsIgnoreCase("string") && !destType.toLowerCase().startsWith("varchar")
                        && !destType.toLowerCase().startsWith("nvarchar")) {
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
            if (metadata.getApprovedUsage() != null
                    && metadata.getApprovedUsage().contains(ApprovedUsage.NONE.getName())) {
                return null;
            } else {
                metadata.setApprovedUsage(Arrays.asList(ApprovedUsage.NONE.getName()));
                log.error("Found unknown VDB statistical type: StatisticalType=" + vdbStatisticalType);
                return null;
            }
        }
    }

}
