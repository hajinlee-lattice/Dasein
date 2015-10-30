package com.latticeengines.metadata.service.impl;

import javax.servlet.http.HttpServletRequest;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;
import com.latticeengines.security.exposed.InternalResourceBase;

@Aspect
public class ResourceAspect {

    @Autowired
    private TableResourceHelper tableResourceHelper;
    
    @Autowired
    private TableTypeHolder tableTypeHolder;
    
    private InternalResourceBase internalResourceBase = new InternalResourceBase();

    @Before("execution(* com.latticeengines.metadata.controller.ImportTableResource.*(..))")
    public void allMethodsForImportTableResource(JoinPoint joinPoint) {
        checkHeader(joinPoint);
        setTableType(joinPoint, TableType.IMPORTTABLE);
    }
    
    @Before("execution(* com.latticeengines.metadata.controller.MetadataResource.*(..))")
    public void allMethodsForTableResource(JoinPoint joinPoint) {
        checkHeader(joinPoint);
        setTableType(joinPoint, TableType.DATATABLE);
    }
    
    private void checkHeader(JoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        
        for (Object arg : args) {
            if (arg instanceof HttpServletRequest) {
                internalResourceBase.checkHeader((HttpServletRequest) arg);
            }
        }
    }

    private void setTableType(JoinPoint joinPoint, TableType tableType) {
        tableTypeHolder.setTableType(tableType);
        Object[] args = joinPoint.getArgs();
        
        for (Object arg : args) {
            if (arg instanceof Table) {
                ((Table) arg).setTableType(tableType);
            }
        }
    }
    
}
