package com.latticeengines.objectapi.object;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.query.exposed.object.BusinessObject;

@Component("accountObject")
public class AccountObject extends BusinessObject {
    private static final Log log = LogFactory.getLog(AccountObject.class);
}