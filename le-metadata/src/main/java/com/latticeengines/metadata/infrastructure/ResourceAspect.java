package com.latticeengines.metadata.infrastructure;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.metadata.entitymgr.impl.TableTypeHolder;

@Aspect
public class ResourceAspect {

    @Autowired
    private TableTypeHolder tableTypeHolder;

    @Around("execution(* com.latticeengines.metadata.controller.ImportTableResource.*(..))")
    public Object allMethodsForImportTableResource(ProceedingJoinPoint joinPoint) throws Throwable {
        setTableType(joinPoint, TableType.IMPORTTABLE);
        Object obj = joinPoint.proceed();
        setTableType(joinPoint, TableType.DATATABLE);
        return obj;
    }

    @Before("execution(* com.latticeengines.metadata.controller.TableResource.*(..))")
    public void allMethodsForTableResource(JoinPoint joinPoint) {
        setTableType(joinPoint, TableType.DATATABLE);
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
