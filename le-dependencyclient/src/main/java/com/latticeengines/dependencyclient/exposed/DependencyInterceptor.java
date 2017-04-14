package com.latticeengines.dependencyclient.exposed;

import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.event.spi.PostLoadEvent;
import org.hibernate.event.spi.PostLoadEventListener;
import org.hibernate.internal.SessionFactoryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dependency.Dependable;
import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.proxy.exposed.metadata.DependableObjectProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class DependencyInterceptor implements PostLoadEventListener, PersistEventListener {
    private static final Log log = LogFactory.getLog(DependencyInterceptor.class);

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private DependableObjectProxy dependableObjectProxy;

    private boolean enabled = true;

    @PostConstruct
    private void init() {
        EventListenerRegistry registry = ((SessionFactoryImpl) sessionFactory).getServiceRegistry().getService(
                EventListenerRegistry.class);
        registry.appendListeners(EventType.POST_LOAD, this);
        registry.appendListeners(EventType.PERSIST, this);
    }

    @Override
    public void onPostLoad(PostLoadEvent event) {
        if (enabled) {
            if (event.getEntity() instanceof Dependable && !(event.getEntity() instanceof DependableObject)) {
                Dependable dependable = (Dependable) event.getEntity();
                log.info(String.format("Retrieving dependencies for [name %s, type %s]", dependable.getName(),
                        dependable.getType()));
                DependableObject object = dependableObjectProxy.find(MultiTenantContext.getCustomerSpace().toString(),
                        dependable.getType().toString(), dependable.getName());
                if (object != null) {
                    dependable.setDependencies(object.getDependencies());
                }
            }
        }
    }

    @Override
    public void onPersist(PersistEvent event) throws HibernateException {
        if (enabled) {
            if (event.getObject() instanceof Dependable && !(event.getObject() instanceof DependableObject)) {
                Dependable dependable = (Dependable) event.getObject();
                log.info(String.format("Updating dependencies for [name %s, type %s, #dependencies %d]",
                        dependable.getName(), dependable.getType(), dependable.getDependencies().size()));
                DependableObject object = new DependableObject();
                object.setName(dependable.getName());
                object.setType(dependable.getType());
                object.setDependencies(dependable.getDependencies());
                dependableObjectProxy.createOrUpdate(MultiTenantContext.getCustomerSpace().toString(), object);
            }
        }
    }

    @Override
    public void onPersist(PersistEvent event, Map createdAlready) throws HibernateException {
        onPersist(event);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
