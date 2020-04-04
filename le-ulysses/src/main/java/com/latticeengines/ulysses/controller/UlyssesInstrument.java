package com.latticeengines.ulysses.controller;

import org.aspectj.lang.reflect.MethodSignature;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;

public class UlyssesInstrument implements InvocationInstrument {

    private final ColumnSelection.Predefined attrGroup;
    private final boolean allowWithoutAttrGroup;

    UlyssesInstrument(ColumnSelection.Predefined attrGroup) {
        this.attrGroup = attrGroup;
        this.allowWithoutAttrGroup = false;
    }

    UlyssesInstrument(boolean allowWithoutAttrGroup) {
        this.allowWithoutAttrGroup = allowWithoutAttrGroup;
        this.attrGroup = null;
    }

    @Override
    public boolean accept(MethodSignature signature, Object[] args, Object toReturn, Throwable ex) {
        return allowWithoutAttrGroup || this.attrGroup.equals(args[2]);
    }

    @Override
    public String getTenantId(MethodSignature signature, Object[] args) {
        return MultiTenantContext.getShortTenantId();
    }

}
