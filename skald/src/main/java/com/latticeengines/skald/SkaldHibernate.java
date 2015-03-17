package com.latticeengines.skald;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SkaldHibernate implements InitializingBean {
    @Override
    public void afterPropertiesSet() throws Exception {
        // Alas, this seems to be required in the AWS Tomcat setup.
        Class.forName("org.postgresql.Driver");

        // TODO Update to the latest version of Hibernate.

        // TODO Configure connection pooling.

        Configuration configuration = new Configuration();
        configuration.setProperty("hibernate.connection.url", properties.getHistoryAddress());
        configuration.setProperty("hibernate.connection.username", properties.getHistoryUser());
        configuration.setProperty("hibernate.connection.password", properties.getHistoryPassword());
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

    private ServiceRegistry registry;
    private SessionFactory factory;
}
