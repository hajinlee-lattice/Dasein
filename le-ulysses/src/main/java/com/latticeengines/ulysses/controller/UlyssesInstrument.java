package com.latticeengines.ulysses.controller;

import org.aspectj.lang.reflect.MethodSignature;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.monitor.exposed.annotation.InvocationInstrument;

public class UlyssesInstrument implements InvocationInstrument {

    private final ColumnSelection.Predefined attrGroup;

    UlyssesInstrument(ColumnSelection.Predefined attrGroup) {
        this.attrGroup = attrGroup;
    }

    @Override
    public boolean accept(MethodSignature signature, Object[] args, Object toReturn, Throwable ex) {
        ColumnSelection.Predefined attrGroup = (ColumnSelection.Predefined) args[2];
        return this.attrGroup.equals(attrGroup);
    }

    @Override
    public String getTenantId(MethodSignature signature, Object[] args) {
        return MultiTenantContext.getShortTenantId();
    }

}
