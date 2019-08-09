package com.latticeengines.apps.cdl.util;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ImportTemplateDiagnostic;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TableUtils;

public class DiagnoseTable {

    public static ImportTemplateDiagnostic diagnostic(String customerSpaceStr, Table table, BusinessEntity businessEntity,
                                                      BatonService batonService) {
        ImportTemplateDiagnostic diagnostic = new ImportTemplateDiagnostic();
        // 1. check physical data type & fundamental type
        for (
                Attribute attr : table.getAttributes()) {
            try {
                Schema.Type avroType = TableUtils.getTypeFromPhysicalDataType(attr.getPhysicalDataType());
                if (StringUtils.isEmpty(attr.getFundamentalType())) {
                    diagnostic.addWarnings(String.format("Attribute %s has data type %s but has empty fundamental " +
                            "type.", attr.getName(), attr.getPhysicalDataType()));
                    continue;
                }
                FundamentalType fdType = FundamentalType.fromName(attr.getFundamentalType());
                switch (avroType) {
                    case ENUM:
                        if (!FundamentalType.ENUM.equals(fdType)) {
                            diagnostic.addWarnings(String.format("Attribute %s has data type %s but fundamental " +
                                    "type is %s", attr.getName(), attr.getPhysicalDataType(), attr.getFundamentalType()));
                        }
                        break;
                    case STRING:
                        if (!FundamentalType.ALPHA.equals(fdType) && !FundamentalType.DATE.equals(fdType)) {
                            diagnostic.addWarnings(String.format("Attribute %s has data type %s but fundamental " +
                                    "type is %s", attr.getName(), attr.getPhysicalDataType(), attr.getFundamentalType()));
                        }
                        break;
                    case INT:
                    case FLOAT:
                        if (!FundamentalType.NUMERIC.equals(fdType)) {
                            diagnostic.addWarnings(String.format("Attribute %s has data type %s but fundamental " +
                                    "type is %s", attr.getName(), attr.getPhysicalDataType(), attr.getFundamentalType()));
                        }
                        break;
                    case LONG:
                    case DOUBLE:
                        if (!FundamentalType.NUMERIC.equals(fdType)
                                && !FundamentalType.CURRENCY.equals(fdType)
                                && !FundamentalType.DATE.equals(fdType)) {
                            diagnostic.addWarnings(String.format("Attribute %s has data type %s but fundamental " +
                                    "type is %s", attr.getName(), attr.getPhysicalDataType(), attr.getFundamentalType()));
                        }
                        break;
                    case BOOLEAN:
                        if (!FundamentalType.BOOLEAN.equals(fdType)) {
                            diagnostic.addWarnings(String.format("Attribute %s has data type %s but fundamental " +
                                    "type is %s", attr.getName(), attr.getPhysicalDataType(), attr.getFundamentalType()));
                        }
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                diagnostic.addErrors(String.format("Attribute %s has wrong data type %s", attr.getName(),
                        attr.getPhysicalDataType()));
            }
        }
        // 2. with standard schema:
        if (businessEntity != null) {
            boolean entityMatch = batonService.isEntityMatchEnabled(CustomerSpace.parse(customerSpaceStr));
            Table standardTable = SchemaRepository.instance().getSchema(businessEntity, true, false, entityMatch);
            Map<String, Attribute> standardAttrMap =
                    standardTable.getAttributes().stream().collect(Collectors.toMap(Attribute::getName, attr -> attr));

            for (Attribute attr : table.getAttributes()) {
                if (standardAttrMap.containsKey(attr.getName())) {
                    Attribute standardAttr = standardAttrMap.get(attr.getName());
                    if (!attr.getPhysicalDataType().equalsIgnoreCase(standardAttr.getPhysicalDataType())) {
                        diagnostic.addErrors(String.format("Attribute %s has wrong physicalDataType %s, should be %s",
                                attr.getName(), attr.getPhysicalDataType(), standardAttr.getPhysicalDataType()));
                    }
                    if (!attr.getRequired().equals(standardAttr.getRequired())) {
                        diagnostic.addErrors(String.format("Attribute %s has required flag %b, is different from schema."
                                , attr.getName(), attr.getRequired()));
                    }
                    if (!attr.isNullable().equals(standardAttr.isNullable())) {
                        diagnostic.addErrors(String.format("Attribute %s has nullable flag %b, is different from schema."
                                , attr.getName(), attr.isNullable()));
                    }
                }
            }
        }
        return diagnostic;
    }
}
