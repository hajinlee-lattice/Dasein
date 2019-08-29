package com.latticeengines.domain.exposed.query;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AccountMaster;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedPeriodTransaction;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.AggregatedTransaction;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.BucketedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CalculatedCuratedAccountAttribute;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CalculatedDepivotedPurchaseHistory;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.CalculatedPurchaseHistory;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedContact;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedDailyTransaction;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedPeriodTransaction;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedProduct;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.PivotedRating;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.SortedContact;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.SortedProduct;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.SortedProductHierarchy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public enum BusinessEntity implements GraphNode {
    // Customer Data Lake
    Account, //
    Contact, //
    Product, //
    Transaction, //
    PeriodTransaction, //
    PurchaseHistory, //
    DepivotedPurchaseHistory, //

    // The Business Entity below stores a special class of derived attributes
    // which are computed based on other
    // attributes from Account, Contact, Product, and Transaction.
    // CuratedAccount only covers Account-level
    // derived attributes which are indexed by Account ID, but other Business
    // Entities can be added for curated
    // attributes based on Contact, Product, etc.
    CuratedAccount, //

    AnalyticPurchaseState,

    Rating, //

    // Lattice Data Cloud
    LatticeAccount, //
    ProductHierarchy,

    // WebActivity
    ActivityStream,
    Catalog;

    public static final Set<BusinessEntity> SEGMENT_ENTITIES = //
            ImmutableSet.of(Account, Contact, PurchaseHistory, Rating, CuratedAccount);
    public static final Set<BusinessEntity> EXPORT_ENTITIES = //
            ImmutableSet.of(Account, Contact, PurchaseHistory, Rating, CuratedAccount);
    public static final Set<BusinessEntity> COUNT_ENTITIES = ImmutableSet.of(Account, Contact);
    public static final Set<BusinessEntity> COMPANY_PROFILE_ENTITIES = ImmutableSet.of(Account, PurchaseHistory, Rating,
            CuratedAccount);
    public static final Set<BusinessEntity> MODELING_ENTITIES = ImmutableSet.of(Account, AnalyticPurchaseState);

    static {
        // Storage
        Account.setBatchStore(ConsolidatedAccount);
        Account.setServingStore(BucketedAccount);

        Contact.setBatchStore(ConsolidatedContact);
        Contact.setServingStore(SortedContact);

        Product.setBatchStore(ConsolidatedProduct);
        Product.setServingStore(SortedProduct);

        Transaction.setBatchStore(ConsolidatedDailyTransaction);
        Transaction.setServingStore(AggregatedTransaction);

        PeriodTransaction.setBatchStore(ConsolidatedPeriodTransaction);
        PeriodTransaction.setServingStore(AggregatedPeriodTransaction);

        PurchaseHistory.setServingStore(CalculatedPurchaseHistory);

        DepivotedPurchaseHistory.setServingStore(CalculatedDepivotedPurchaseHistory);

        CuratedAccount.setServingStore(CalculatedCuratedAccountAttribute);

        AnalyticPurchaseState.setServingStore(TableRoleInCollection.AnalyticPurchaseState);

        Rating.setServingStore(PivotedRating);

        LatticeAccount.setServingStore(AccountMaster);

        ProductHierarchy.setBatchStore(ConsolidatedProduct);
        ProductHierarchy.setServingStore(SortedProductHierarchy);

        // Relationships
        Account.addRelationship(Contact, Cardinality.ONE_TO_MANY, InterfaceName.AccountId);
        Account.addRelationship(Transaction, Cardinality.ONE_TO_MANY, InterfaceName.AccountId);
        Account.addRelationship(Rating, Cardinality.ONE_TO_ONE, InterfaceName.AccountId);
        Account.addRelationship(PurchaseHistory, Cardinality.ONE_TO_ONE, InterfaceName.AccountId);
        Account.addRelationship(DepivotedPurchaseHistory, Cardinality.ONE_TO_MANY, InterfaceName.AccountId);
        Account.addRelationship(CuratedAccount, Cardinality.ONE_TO_ONE, InterfaceName.AccountId);

        Contact.addRelationship(Account, Cardinality.MANY_TO_ONE, InterfaceName.AccountId);

        Product.addRelationship(Transaction, Cardinality.ONE_TO_MANY, InterfaceName.ProductId);
        Product.addRelationship(PurchaseHistory, Cardinality.ONE_TO_MANY, InterfaceName.ProductId);

        Transaction.addRelationship(Account, Cardinality.MANY_TO_ONE, InterfaceName.AccountId);
        PeriodTransaction.addRelationship(Account, Cardinality.MANY_TO_ONE, InterfaceName.AccountId);
        ProductHierarchy.addRelationship(Transaction, Cardinality.ONE_TO_MANY, InterfaceName.ProductId);
        PurchaseHistory.addRelationship(Account, Cardinality.MANY_TO_ONE, InterfaceName.AccountId);
        DepivotedPurchaseHistory.addRelationship(Account, Cardinality.MANY_TO_ONE, InterfaceName.AccountId);
        DepivotedPurchaseHistory.addRelationship(Account, Cardinality.MANY_TO_ONE, InterfaceName.AccountId);
        CuratedAccount.addRelationship(Account, Cardinality.ONE_TO_ONE, InterfaceName.AccountId);
    }

    // Entity Definitions
    private TableRoleInCollection batchStore;
    private TableRoleInCollection servingStore;
    private List<Relationship> relationships = new ArrayList<>();

    public static BusinessEntity getByName(String entity) {
        for (BusinessEntity businessEntity : values()) {
            if (businessEntity.name().equalsIgnoreCase(entity)) {
                return businessEntity;
            }
        }
        throw new IllegalArgumentException(String.format("There is no entity name %s in BusinessEntity", entity));
    }

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
                Relationship join = parent.relationships.stream().filter(r -> r.child.equals(entity)).findFirst()
                        .orElse(null);
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

    public enum Cardinality {
        ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY
    }

    public enum DataStore {
        Batch, Serving
    }

    public static class Relationship {
        private final BusinessEntity parent;
        private final BusinessEntity child;
        private final Cardinality cardinality;
        private final List<Pair<InterfaceName, InterfaceName>> joinKeys;

        Relationship(BusinessEntity parent, BusinessEntity child, Cardinality cardinality, InterfaceName joinKey) {
            this(parent, child, cardinality, Collections.singletonList(Pair.of(joinKey, joinKey)));
        }

        Relationship(BusinessEntity parent, BusinessEntity child, Cardinality cardinality,
                List<Pair<InterfaceName, InterfaceName>> joinKeys) {
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

        public Cardinality getCardinality() {
            return cardinality;
        }
    }
}
