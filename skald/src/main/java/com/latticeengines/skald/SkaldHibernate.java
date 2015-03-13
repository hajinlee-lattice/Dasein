package com.latticeengines.skald;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SkaldHibernate {
    public SkaldHibernate() {
        // TODO Update to the latest version of Hibernate.

        // TODO Configure connection pooling.

        Configuration configuration = new Configuration();

        // TODO Extract configuration from properties.
        configuration.setProperty("hibernate.connection.url", "jdbc:postgresql://localhost:5432/rts");
        configuration.setProperty("hibernate.connection.username", "postgres");
        configuration.setProperty("hibernate.connection.password", "postgres");
        configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");

        // TODO Automatically scan for annotated classes.
        configuration.addAnnotatedClass(ScoreHistoryEntry.class);

        registry = new ServiceRegistryBuilder().applySettings(configuration.getProperties()).buildServiceRegistry();

        factory = configuration.buildSessionFactory(registry);
    }

    public Session openSession() {
        return factory.openSession();
    }

    @Autowired
    private SkaldProperties properties;

    private final ServiceRegistry registry;
    private final SessionFactory factory;
}
