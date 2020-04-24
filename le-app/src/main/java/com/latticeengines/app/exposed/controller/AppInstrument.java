package com.latticeengines.app.exposed.controller;

import org.aspectj.lang.reflect.MethodSignature;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;

public class AppInstrument implements InvocationInstrument {

    private final ColumnSelection.Predefined attrGroup;
    private final boolean allowWithoutAttrGroup;

    AppInstrument(ColumnSelection.Predefined attrGroup) {
        this.attrGroup = attrGroup;
        this.allowWithoutAttrGroup = false;
    }

    AppInstrument(boolean allowWithoutAttrGroup) {
        this.allowWithoutAttrGroup = allowWithoutAttrGroup;
        this.attrGroup = null;
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public boolean accept(MethodSignature signature, Object[] args, Object toReturn, Throwable ex) {
        return allowWithoutAttrGroup || this.attrGroup.equals(args[2]);
    }

    @Override
    public String getTenantId(MethodSignature signature, Object[] args) {
        return MultiTenantContext.getShortTenantId();
    }

}
