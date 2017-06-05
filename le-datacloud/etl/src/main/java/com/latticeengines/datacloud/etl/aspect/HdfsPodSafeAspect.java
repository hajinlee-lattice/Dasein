package com.latticeengines.datacloud.etl.aspect;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.datacloud.core.util.HdfsPodContext;

@Aspect
public class HdfsPodSafeAspect {

    @Autowired
    @Qualifier(value = "sessionFactoryPropDataManage")
    private SessionFactory sessionFactory;

    @Before("execution(* com.latticeengines.datacloud.etl.publication.entitymgr.impl.PublicationProgressEntityMgrImpl.find*(..))")
    public void findPublicationProgress(JoinPoint joinPoint) {
        enableHdfsPodFilter(joinPoint);
    }

    @Before("execution(* com.latticeengines.datacloud.etl.ingestion.entitymgr.impl.IngestionProgressEntityMgrImpl.find*(..))")
    public void findIngestionProgress(JoinPoint joinPoint) {
        enableHdfsPodFilter(joinPoint);
    }

    @Before("execution(* com.latticeengines.datacloud.etl.ingestion.entitymgr.impl.IngestionProgressEntityMgrImpl.is*(..))")
    public void isIngestionProgress(JoinPoint joinPoint) {
        enableHdfsPodFilter(joinPoint);
    }

    @Before("execution(* com.latticeengines.datacloud.etl.transformation.entitymgr.impl.TransformationProgressEntityMgrImpl.find*(..))")
    public void findTransformationProgress(JoinPoint joinPoint) {
        enableHdfsPodFilter(joinPoint);
    }

    @Before("execution(* com.latticeengines.datacloud.etl.transformation.entitymgr.impl.TransformationProgressEntityMgrImpl.has*(..))")
    public void hasTransformationProgress(JoinPoint joinPoint) {
        enableHdfsPodFilter(joinPoint);
    }

    private void enableHdfsPodFilter(JoinPoint joinPoint) {
        Session session = sessionFactory.getCurrentSession();
        if (session == null) {
            throw new RuntimeException("There is no current session.");
        }
        session.enableFilter("hdfsPodFilter").setParameter("hdfsPod", HdfsPodContext.getHdfsPodId());
    }

}
