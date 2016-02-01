package com.latticeengines.pls.entitymanager.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.AmAttribute;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.pls.dao.AmAttributeDao;
import com.latticeengines.pls.entitymanager.AmAttributeEntityMgr;
import com.latticeengines.workflow.exposed.entitymgr.ReportEntityMgr;

@Component("amAttributeEntityMgr")
public class AmAttributeEntityMgrImpl extends BaseEntityMgrImpl<AmAttribute> implements AmAttributeEntityMgr {

    private static final Log log = LogFactory.getLog(AmAttributeEntityMgrImpl.class);

    @Autowired
    private AmAttributeDao amAttributeDao;

    @Autowired
    private ReportEntityMgr reportEntityMgr;

    private final String countAttrs[] = { "your_customer_count", "in_your_db_count" };
    private final String liftAttrs[] = { "lift" };

    @Override
    public BaseDao<AmAttribute> getDao() {
        return amAttributeDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AmAttribute> findAttributes(String key, String parentKey, String parentValue, boolean populate) {
        List<AmAttribute> attrs = amAttributeDao.findAttributes(key, parentKey, parentValue);
        if (populate == true) {
            collectProperties(attrs, key, parentKey, parentValue);
        }
        return attrs;
    }

    private void collectProperties(List<AmAttribute> attrs, String key, String parentKey, String parentValue) {
        Map<String, AmAttribute> attrMap = new HashMap<String, AmAttribute>();
        for (AmAttribute attr : attrs) {
            attr.setProperty("CompanyCount", "0");
            for (int i = 0; i < countAttrs.length; i++) {
                attr.setProperty(countAttrs[i], "0");
            }
            for (int i = 0; i < liftAttrs.length; i++) {
                attr.setProperty(liftAttrs[i], "0");
            }
            attr.setProperty("SubCategoryCount", "0");
            attrMap.put(attr.getAttrValue(), attr);
        }
        AmAttribute meta = amAttributeDao.findAttributeMeta(key);
        if (meta.getSource().equals("AccountMaster_Accounts")) {
            collectCompanyCount(attrMap, key, parentKey, parentValue);
            collectSubCategoryCount(attrMap, key, parentKey, parentValue);
            collectCustomerData(attrMap, key, parentKey, parentValue);
        }
    }

    @SuppressWarnings("rawtypes")
    private void collectCompanyCount(Map<String, AmAttribute> attrMap, String key, String parentKey, String parentValue) {
        List<List> list = amAttributeDao.findCompanyCount(key, parentKey, parentValue);
        for (int i = 0; i < list.size(); i++) {
            List property = (List) list.get(i);
            AmAttribute attr = attrMap.get((String) property.get(0));
            attr.setProperty("CompanyCount", property.get(1).toString());
        }
    }

    @SuppressWarnings("rawtypes")
    private void collectSubCategoryCount(Map<String, AmAttribute> attrMap, String key, String parentKey,
            String parentValue) {

        List<List> list = amAttributeDao.findSubCategoryCount(key, parentKey, parentValue);
        for (int i = 0; i < list.size(); i++) {
            List property = (List) list.get(i);
            AmAttribute attr = attrMap.get((String) property.get(0));
            attr.setProperty("SubCategoryCount", property.get(1).toString());
        }
    }

    private void collectCustomerData(Map<String, AmAttribute> attrMap, String key, String parentKey, String parentValue) {

        List<Report> reports = reportEntityMgr.findAll();
        HashMap<String, String> reportMap = new HashMap<String, String>();
        for (Report report : reports)
            reportMap.put(report.getPurpose().getKey(), report.getName());

        String countPurpose = "";
        if (key.equals("SubIndustry")) {
            countPurpose = "CreateAttributeLevelSummary_BusinessIndustry2";
        } else {
            countPurpose = "CreateAttributeLevelSummary_Business" + key;
        }

        String countReportName = reportMap.get(countPurpose);
        if (countReportName != null)
            collectValueFromReport(attrMap, countReportName, countAttrs);

        String liftPurpose = countPurpose + "_Probability";
        String liftReportName = reportMap.get(liftPurpose);
        if (liftReportName != null)
            collectValueFromReport(attrMap, liftReportName, liftAttrs);

    }

    private void collectValueFromReport(Map<String, AmAttribute> attrMap, String reportName, String[] attrs) {

        log.info("Process report " + reportName);
        Report report = reportEntityMgr.findByName(reportName);
        if (report == null)
            return;

        String json = report.getJson().getPayload();
        JsonNode root;
        try {
            root = new ObjectMapper().readTree(json);
        } catch (IOException e) {
            log.info("Invalid json string " + json);
            return;
        }

        JsonNode records = root.get("records");
        if (!records.isArray())
            return;

        for (JsonNode record : records) {
            String attrValue = record.get("value").asText();
            AmAttribute attr = attrMap.get(attrValue);
            if (attr == null)
                continue;
            for (int i = 0; i < attrs.length; i++) {
                JsonNode vNode = record.get(attrs[i]);
                if (vNode == null) {
                    log.info("Failed to find attr " + attrs[i] + " for " + attrValue);
                    continue;
                }
                attr.setProperty(attrs[i], vNode.asText());
            }
        }
    }
}
