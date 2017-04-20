package com.latticeengines.dependencyclient.exposed;

import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventType;
import org.hibernate.event.spi.PersistEvent;
import org.hibernate.event.spi.PersistEventListener;
import org.hibernate.internal.SessionFactoryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dependency.Dependable;
import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.proxy.exposed.metadata.DependableObjectProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class DependencyInterceptor implements PersistEventListener, DeleteEventListener {
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
        registry.appendListeners(EventType.PERSIST, this);
    }

    @Override
    public void onPersist(PersistEvent event) throws HibernateException {
        if (enabled) {
            if (event.getObject() instanceof Dependable && !(event.getObject() instanceof DependableObject)) {
                Dependable dependable = (Dependable) event.getObject();
                log.info(String.format("Updating dependencies for [name %s, type %s, #dependencies %d]", dependable
                        .getDependableName(), dependable.getDependableType(), dependable.getDependencies().size()));
                DependableObject object = DependableObject.fromDependable(dependable);
                dependableObjectProxy.createOrUpdate(MultiTenantContext.getTenant().getId(), object);
            }
        }
    }

    @Override
    public void onPersist(PersistEvent event, Map createdAlready) throws HibernateException {
        onPersist(event);
    }

    @Override
    public void onDelete(DeleteEvent event) throws HibernateException {
        if (enabled) {
            if (event.getObject() instanceof Dependable && !(event.getObject() instanceof DependableObject)) {
                Dependable dependable = (Dependable) event.getObject();
                log.info(String.format("Deleting dependencies for [name %s, type %s, #dependencies %d]", dependable
                        .getDependableName(), dependable.getDependableType(), dependable.getDependencies().size()));
                dependableObjectProxy.delete(MultiTenantContext.getTenant().getId(), //
                        dependable.getDependableType().toString(), dependable.getDependableName());
            }
        }
    }

    @Override
    public void onDelete(DeleteEvent event, Set transientEntities) throws HibernateException {
        onDelete(event);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

}
