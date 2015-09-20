package com.latticeengines.eai.routes.salesforce;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.spring.SpringCamelContext;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;

import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.routes.HdfsUriGenerator;

public class XmlHandlerProcessor implements Processor {

    private SpringCamelContext context;

    public XmlHandlerProcessor(CamelContext context) {
        this.context = (SpringCamelContext) context;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Table table = exchange.getProperty(ImportProperty.TABLE, Table.class);

        String beanName = "extractDataXmlHandlerFor" + table.getName();
        AutowireCapableBeanFactory factory = context.getApplicationContext().getAutowireCapableBeanFactory();
        BeanDefinitionRegistry registry = (BeanDefinitionRegistry) factory;
        GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
        beanDefinition.setBeanClass(ExtractDataXmlHandler.class);
        beanDefinition.setAutowireCandidate(true);
        registry.registerBeanDefinition(beanName, beanDefinition);
        factory.autowireBeanProperties(this, AutowireCapableBeanFactory.AUTOWIRE_BY_TYPE, false);

        ExtractDataXmlHandler handler = context.getApplicationContext().getBean(beanName, ExtractDataXmlHandler.class);
        String fileName = handler.initialize(context, table);
        exchange.getIn().setHeader("staxUri", "stax:#" + beanName);
        String uri = new HdfsUriGenerator().getHdfsUri(exchange, table, fileName);
        exchange.getIn().setHeader("hdfsUri", uri);
    }

}
