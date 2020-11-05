package com.latticeengines.metadata.repository.document;

import org.springframework.data.repository.NoRepositoryBean;

import com.latticeengines.documentdb.entity.DataTemplateEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;

@NoRepositoryBean
public interface DataTemplateRepository extends MultiTenantDocumentRepository<DataTemplateEntity> {

    DataTemplateEntity findByUuid(String uuid);

}
