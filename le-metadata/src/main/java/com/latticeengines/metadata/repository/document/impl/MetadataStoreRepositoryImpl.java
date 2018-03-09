package com.latticeengines.metadata.repository.document.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.criteria.Predicate;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.support.JpaEntityInformation;

import com.latticeengines.db.exposed.repository.impl.BaseJpaRepositoryImpl;
import com.latticeengines.documentdb.entity.NameSpacedDocument;
import com.latticeengines.metadata.repository.document.MetadataStoreRepository;

public class MetadataStoreRepositoryImpl<T extends NameSpacedDocument> extends BaseJpaRepositoryImpl<T, String>
        implements MetadataStoreRepository<T> {

    private final JpaEntityInformation<T, ?> entityInformation;
    private final EntityManager entityManager;

    public MetadataStoreRepositoryImpl(JpaEntityInformation<T, ?> entityInformation, EntityManager entityManager) {
        super(entityInformation, entityManager);
        this.entityInformation = entityInformation;
        this.entityManager = entityManager;
    }

    @Override
    public EntityManager getEntityManager() {
        return entityManager;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<T> findByNamespace(Class<T> clz, Pageable pageable, Serializable... namespace) {
        Specification<T> spec = namespaceSpec(clz, namespace);
        if (pageable == null) {
            return super.findAll(spec);
        } else {
            return super.findAll(pageable).getContent();
        }
    }

    @Override
    public long countByNameSpace(Class<T> clz, Serializable... namespace) {
        Specification<T> spec = namespaceSpec(clz, namespace);
        return super.count(spec);
    }

    private Specification<T> namespaceSpec(Class<T> clz, Serializable... namespace) {
        List<String> keys = getNamespaceKeys(clz);
        if (keys.size() != namespace.length) {
            throw new RuntimeException(String.format("Expecting %d namespace coordinates, but only %d are provided.", //
                    namespace.length, keys.size()));
        }
        Map<String, Serializable> nsParams = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            Serializable value = namespace[i];
            nsParams.put(key, value);
        }

        return (Specification<T>) (root, criteriaQuery, builder) -> {
            List<Predicate> predicates = new ArrayList<>();
            for (Map.Entry<String, Serializable> entry: nsParams.entrySet()) {
                String key = entry.getKey();
                Serializable value = entry.getValue();
                Predicate predicate;
                if (value == null) {
                    predicate = builder.isNull(root.get(key));
                } else {
                    predicate = builder.equal(root.get(key), value);
                }
                predicates.add(predicate);
            }
            return builder.and(predicates.toArray(new Predicate[predicates.size()]));
        };
    }

    @SuppressWarnings("unchecked")
    private List<String> getNamespaceKeys(Class<T> clz) {
        T instance;
        try {
            instance = clz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate an object of class " + clz);
        }
        return instance.getnamespaceKeys();
    }

}