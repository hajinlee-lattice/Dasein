package com.latticeengines.domain.exposed.query;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountMaster;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.SortedContact;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedContact;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public enum BusinessEntity implements GraphNode {
    // Customer Data Lake
    Account,
    Contact,

    // Lattice Data Cloud
    LatticeAccount;

    static {
        // Storages
        Account.setBatchStore(ConsolidatedAccount);
        Account.setServingStore(BucketedAccount);
        Contact.setBatchStore(ConsolidatedContact);
        Contact.setServingStore(SortedContact);

        LatticeAccount.setServingStore(AccountMaster);

        // Relationships
        Account.addRelationship(Contact, Cardinality.ONE_TO_MANY, InterfaceName.AccountId);
        Account.addRelationship(LatticeAccount, Cardinality.ONE_TO_ONE, InterfaceName.LatticeAccountId);
    }

    // Entity Definitions
    private TableRoleInCollection batchStore;
    private TableRoleInCollection servingStore;
    private List<Relationship> relationships = new ArrayList<>();

    public TableRoleInCollection getBatchStore() {
        return batchStore;
    }

    public void setBatchStore(TableRoleInCollection batchStore) {
        this.batchStore = batchStore;
    }

    public TableRoleInCollection getServingStore() {
        return servingStore;
    }

    public void setServingStore(TableRoleInCollection servingStore) {
        this.servingStore = servingStore;
    }

    private void addRelationship(BusinessEntity child, Cardinality cardinality, InterfaceName joinKey) {
        relationships.add(new Relationship(this, child, cardinality, joinKey));
    }

    // check if can find another entity via joins, return minimum hops
    public Relationship join(BusinessEntity target) {
        BreadthFirstSearch search = new BreadthFirstSearch();
        Map<BusinessEntity, Relationship> joinCache = new HashMap<>();
        joinCache.put(this, null);
        search.run(this, (object, ctx) -> {
            BusinessEntity entity = (BusinessEntity) object;
            if (!joinCache.containsKey(entity)) {
                BusinessEntity parent = (BusinessEntity) ctx.getProperty("parent");
                Relationship join = parent.relationships.stream().filter(r -> r.child.equals(entity)).findFirst().orElse(null);
                joinCache.put(entity, join);
            }
        });
        return joinCache.containsKey(target) ? joinCache.get(target) : null;
    }

    @Override
    public List<GraphNode> getChildren() {
        return relationships.stream().map(r -> r.child).collect(Collectors.toList());
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return new HashMap<>();
    }

    public enum Cardinality { ONE_TO_ONE, ONE_TO_MANY, MANY_TO_MANY }

    public static class Relationship {
        private final BusinessEntity parent;
        private final BusinessEntity child;
        private final Cardinality cardinality;
        private final List<Pair<InterfaceName, InterfaceName>> joinKeys;

        Relationship(BusinessEntity parent, BusinessEntity child, Cardinality cardinality, InterfaceName joinKey) {
            this(parent, child, cardinality, Collections.singletonList(Pair.of(joinKey, joinKey)));
        }

        Relationship(BusinessEntity parent, BusinessEntity child, Cardinality cardinality, List<Pair<InterfaceName, InterfaceName>> joinKeys) {
            this.parent = parent;
            this.child = child;
            this.cardinality = cardinality;
            this.joinKeys = joinKeys;
        }

        public BusinessEntity getParent() {
            return parent;
        }

        public BusinessEntity getChild() {
            return child;
        }

        public List<Pair<InterfaceName, InterfaceName>> getJoinKeys() {
            return joinKeys;
        }
    }

}
