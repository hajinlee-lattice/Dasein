package com.latticeengines.camille.config;

import org.apache.zookeeper.ZooDefs;

import com.latticeengines.camille.CamilleTransaction;
import com.latticeengines.camille.DocumentSerializationException;
import com.latticeengines.camille.translators.PathTranslator;
import com.latticeengines.camille.translators.PathTranslatorFactory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class StandardConfigurationTransactionImpl<T extends ConfigurationScope> implements ConfigurationTransactionImpl<T> {
    protected ConfigurationScope scope;
    protected PathTranslator translator;
    protected CamilleTransaction transaction;
    
    public StandardConfigurationTransactionImpl(T scope) {
        this.scope = scope;
        this.translator = PathTranslatorFactory.getTranslator(scope);
        this.transaction = new CamilleTransaction();
    }
    
    public void check(Path path, Document document) {
        Path absolute = translator.getAbsolutePath(path);
        transaction.check(absolute,  document);
    }
    
    public void create(Path path, Document document) throws DocumentSerializationException {
        Path absolute = translator.getAbsolutePath(path);
        transaction.create(absolute, document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }
    
    public void set(Path path, Document document) throws DocumentSerializationException {
        Path absolute = translator.getAbsolutePath(path);
        transaction.set(absolute, document);
    }
    
    public void delete(Path path) {
        Path absolute = translator.getAbsolutePath(path);
        transaction.delete(absolute);
    }
    
    public void commit() throws Exception {
        transaction.commit();
    }
}
