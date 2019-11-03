package com.latticeengines.domain.exposed.pls.cdl.channel;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public enum AudienceType {

    ACCOUNTS("Accounts") {
        @Override
        public BusinessEntity asBusinessEntity() {
            return BusinessEntity.Account;
        }

        @Override
        public String getInterfaceName() {
            return InterfaceName.AccountId.name();
        }
    }, //
    CONTACTS("Contacts") {
        @Override
        public BusinessEntity asBusinessEntity() {
            return BusinessEntity.Contact;
        }

        @Override
        public String getInterfaceName() {
            return InterfaceName.ContactId.name();
        }
    };

    private String type;

    AudienceType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public BusinessEntity asBusinessEntity() {
        throw new UnsupportedOperationException("Unsupported Business Entity");
    }

    public String getInterfaceName() {
        throw new UnsupportedOperationException("Unsupported Business Entity");
    }
}
