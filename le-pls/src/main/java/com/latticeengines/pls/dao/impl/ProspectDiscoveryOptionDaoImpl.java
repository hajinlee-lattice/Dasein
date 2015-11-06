package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.pls.dao.ProspectDiscoveryOptionDao;

@Component("prospectDiscoveryOptionDao")
public class ProspectDiscoveryOptionDaoImpl extends BaseDaoImpl<ProspectDiscoveryOption> implements
        ProspectDiscoveryOptionDao {

    @Override
    protected Class<ProspectDiscoveryOption> getEntityClass() {
        return ProspectDiscoveryOption.class;
    }

    @Override
    public ProspectDiscoveryOption findProspectDiscoveryOption(String option) {
        List<ProspectDiscoveryOption> prospectDiscoveryOptions = this.findAll();
        if (prospectDiscoveryOptions.size() == 0) {
            return null;
        }
        for (ProspectDiscoveryOption prospectDiscoveryOption : prospectDiscoveryOptions) {
            if (prospectDiscoveryOption.getOption().equals(option)) {
                return prospectDiscoveryOption;
            }
        }
        return null;
    }

}
