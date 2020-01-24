package com.latticeengines.metadata.fix;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.entitymgr.AttributeEntityMgr;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;

public class FixAttribute extends MetadataFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(FixAttribute.class);

    @Inject
    private AttributeEntityMgr attributeEntityMgr;

    @Inject
    private JdbcTemplate jdbcTemplate;

    @Test(groups = "manual")
    public void test() {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse("SmartBearSoftware").toString());
        MultiTenantContext.setTenant(tenant);
        Table t = tableEntityMgr.findByName("copy_68d4c341_c94e_473c_bd82_3de26b6c6e9f");
        Attribute a = t.getAttribute("State");
        System.out.println(a.getPid());
        a.setInterfaceName(InterfaceName.State);
        attributeEntityMgr.update(a);
    }

    @Test(groups = "manual_fix")
    public void fixTemplate() {
        String tenantId = CustomerSpace.parse("VMware_Atlas").toString();
        Map<String, Attribute> accountAttrs = SchemaRepository.instance().getSchema(BusinessEntity.Account)
                .getAttributes().stream().collect(Collectors.toMap(Attribute::getName, attr -> attr));
        Map<String, Attribute> contactAttrs = SchemaRepository.instance().getSchema(BusinessEntity.Contact)
                .getAttributes().stream().collect(Collectors.toMap(Attribute::getName, attr -> attr));
        Map<String, Attribute> transactionAttrs = SchemaRepository.instance().getSchema(BusinessEntity.Transaction)
                .getAttributes().stream().collect(Collectors.toMap(Attribute::getName, attr -> attr));
        Map<String, Attribute> productAttrs = SchemaRepository.instance().getSchema(BusinessEntity.Product)
                .getAttributes().stream().collect(Collectors.toMap(Attribute::getName, attr -> attr));
        TableType tableType = tableTypeHolder.getTableType();
        tableTypeHolder.setTableType(TableType.IMPORTTABLE);
        String sql = String.format("select t.TENANT_PID, t.TENANT_ID, dft.ENTITY, mt.NAME, mt.PID " +
                "from PLS_MultiTenant.METADATA_TABLE mt " +
                "inner join PLS_MultiTenant.DATAFEED_TASK dft on mt.PID = dft.FK_TEMPLATE_ID " +
                "inner join PLS_MultiTenant.TENANT t on mt.FK_TENANT_ID = t.TENANT_PID " +
                "where t.TENANT_ID = '%s'", tenantId);
        List<Map<String, Object>> rawList = jdbcTemplate.queryForList(sql);
        if (CollectionUtils.isNotEmpty(rawList)) {
            for (Map<String, Object> row : rawList) {
                BusinessEntity entity = BusinessEntity.getByName((String)row.get("ENTITY"));
                Tenant tenant = tenantEntityMgr.findByTenantId((String)row.get("TENANT_ID"));
                MultiTenantContext.setTenant(tenant);
                List<Attribute> templateAttrs = tableEntityMgr.findAttributesByTable_Pid((Long) row.get("PID"), null);

                if (CollectionUtils.isEmpty(templateAttrs)) {
                    continue;
                }
                switch (entity) {
                    case Account:
                        for(Attribute attr : templateAttrs) {
                            if (accountAttrs.containsKey(attr.getName())) {
                                Attribute standardAttr = accountAttrs.get(attr.getName());
                                if (!compareAttribute(attr, standardAttr)) {
                                    log.warn(String.format("Attribute %s with PID %d is not correct!", attr
                                            .getName(), attr.getPid()));
                                    attr.setNullable(standardAttr.getNullable());
                                    attr.setRequired(standardAttr.getRequired());
                                    attr.setInterfaceName(standardAttr.getInterfaceName());
                                    attr.setPhysicalDataType(standardAttr.getPhysicalDataType());
                                    attributeEntityMgr.update(attr);
                                }
                            }
                        }
                        break;
                    case Contact:
                        for(Attribute attr : templateAttrs) {
                            if (contactAttrs.containsKey(attr.getName())) {
                                Attribute standardAttr = contactAttrs.get(attr.getName());
                                if (!compareAttribute(attr, standardAttr)) {
                                    log.warn(String.format("Attribute %s with PID %d is not correct!", attr
                                            .getName(), attr.getPid()));
                                    attr.setNullable(standardAttr.getNullable());
                                    attr.setRequired(standardAttr.getRequired());
                                    attr.setInterfaceName(standardAttr.getInterfaceName());
                                    attr.setPhysicalDataType(standardAttr.getPhysicalDataType());
                                    attributeEntityMgr.update(attr);
                                }
                            }
                        }
                        break;
                    case Transaction:
                        for(Attribute attr : templateAttrs) {
                            if (transactionAttrs.containsKey(attr.getName())) {
                                Attribute standardAttr = transactionAttrs.get(attr.getName());
                                if (!compareAttribute(attr, standardAttr)) {
                                    log.warn(String.format("Attribute %s with PID %d is not correct!", attr
                                            .getName(), attr.getPid()));
                                    attr.setNullable(standardAttr.getNullable());
                                    attr.setRequired(standardAttr.getRequired());
                                    attr.setInterfaceName(standardAttr.getInterfaceName());
                                    attr.setPhysicalDataType(standardAttr.getPhysicalDataType());
                                    attributeEntityMgr.update(attr);
                                }
                            }
                        }
                        break;
                    case Product:
                        for(Attribute attr : templateAttrs) {
                            if (productAttrs.containsKey(attr.getName())) {
                                Attribute standardAttr = productAttrs.get(attr.getName());
                                if (!compareAttribute(attr, standardAttr)) {
                                    log.warn(String.format("Attribute %s with PID %d is not correct!", attr
                                            .getName(), attr.getPid()));
                                    attr.setNullable(standardAttr.getNullable());
                                    attr.setInterfaceName(standardAttr.getInterfaceName());
                                    attr.setPhysicalDataType(standardAttr.getPhysicalDataType());
                                    attributeEntityMgr.update(attr);
                                }
                            }
                        }
                        break;
                    case DepivotedPurchaseHistory:
                    case PeriodTransaction:
                    case ProductHierarchy:
                    case PurchaseHistory:
                    case LatticeAccount:
                    default:
                        break;
                }
            }
        }
        tableTypeHolder.setTableType(tableType);
    }

    private boolean compareAttribute(Attribute attr1, Attribute attr2) {
        if (!attr1.getPhysicalDataType().equalsIgnoreCase(attr2.getPhysicalDataType())) {
            if (InterfaceName.Amount.equals(attr1.getInterfaceName())
                    || InterfaceName.Quantity.equals(attr1.getInterfaceName())
                    || InterfaceName.Cost.equals(attr1.getInterfaceName()))
            {
                if (!attr2.getPhysicalDataType().equalsIgnoreCase("int")
                        && !attr2.getPhysicalDataType().equalsIgnoreCase("double")) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if (!attr1.getRequired().equals(attr2.getRequired())) {
            return false;
        }
        if (attr1.getInterfaceName() == null || attr2.getInterfaceName() == null) {
            return false;
        } else if (!attr1.getInterfaceName().equals(attr2.getInterfaceName())) {
            return false;
        }
        return true;
    }
}
