package com.latticeengines.pls.entitymanager.impl;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.AmAttribute;
import com.latticeengines.pls.dao.AmAttributeDao;
import com.latticeengines.pls.entitymanager.AmAttributeEntityMgr;

@Component("amAttributeEntityMgr")
public class AmAttributeEntityMgrImpl extends BaseEntityMgrImpl<AmAttribute> implements AmAttributeEntityMgr {

    @Autowired
    private AmAttributeDao amAttributeDao;

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
        for (AmAttribute attr: attrs) {
	     attr.setProperty("CompanyCount", "0");
             attrMap.put(attr.getAttrValue(), attr);
        }
        AmAttribute meta = amAttributeDao.findAttributeMeta(key);
        if (meta.getParentValue().equals("Account")) {
            List<List> list = amAttributeDao.findCompanyCount(key, parentKey, parentValue);
            for (int i = 0; i < list.size(); i++) {
                List property = (List)list.get(i);
                AmAttribute attr =  attrMap.get((String)property.get(0));
                attr.setProperty("CompanyCount", property.get(1).toString());
            }
        }
    }
}
