package com.latticeengines.camille.exposed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.zookeeper.data.ACL;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

/**
 * Simple wrapper over CuratorTransaction
 */
public class CamilleTransaction {
    private CuratorTransaction transaction;
    private List<Operation> operations;

    private abstract static class Operation {
        protected Path path;

        Operation(Path path) {
            this.path = path;
        }

        public abstract CuratorTransactionBridge getCuratorEquivalent(CuratorTransaction transaction) throws Exception;

        public abstract CuratorTransactionBridge getCuratorEquivalent(CuratorTransactionBridge bridge) throws Exception;

        public abstract void updateWithResult(CuratorTransactionResult result);

        public abstract OperationType getOperationType();

        public Path getPath() {
            return path;
        }

    }

    private static class DeleteOperation extends Operation {
        DeleteOperation(Path path) {
            super(path);
        }

        @Override
        public CuratorTransactionBridge getCuratorEquivalent(CuratorTransaction transaction) throws Exception {
            return transaction.delete().forPath(path.toString());
        }

        @Override
        public CuratorTransactionBridge getCuratorEquivalent(CuratorTransactionBridge bridge) throws Exception {
            return bridge.and().delete().forPath(path.toString());
        }

        @Override
        public void updateWithResult(CuratorTransactionResult result) {
            // nothing to do
        }

        @Override
        public OperationType getOperationType() {
            return OperationType.DELETE;
        }
    }

    private static class CheckOperation extends Operation {
        private Document document;

        CheckOperation(Path path, Document document) {
            super(path);
            this.document = document;
        }

        @Override
        public CuratorTransactionBridge getCuratorEquivalent(CuratorTransaction transaction) throws Exception {
            return transaction.check().withVersion(document.getVersion()).forPath(path.toString());
        }

        @Override
        public CuratorTransactionBridge getCuratorEquivalent(CuratorTransactionBridge bridge) throws Exception {
            return bridge.and().check().withVersion(document.getVersion()).forPath(path.toString());
        }

        @Override
        public void updateWithResult(CuratorTransactionResult result) {
            // nothing to do
        }

        @Override
        public OperationType getOperationType() {
            return OperationType.CHECK;
        }
    }

    private static class CreateOperation extends Operation {
        private Document document;
        private List<ACL> acl;
        private byte[] data;

        CreateOperation(Path path, Document document, List<ACL> acl) {
            super(path);
            this.document = document;
            this.acl = acl;
            this.data = document.getData().getBytes();
        }

        @Override
        public CuratorTransactionBridge getCuratorEquivalent(CuratorTransaction transaction) throws Exception {
            return transaction.create().withACL(acl).forPath(path.toString(), data);
        }

        @Override
        public CuratorTransactionBridge getCuratorEquivalent(CuratorTransactionBridge bridge) throws Exception {
            return bridge.and().create().withACL(acl).forPath(path.toString(), data);
        }

        @Override
        public void updateWithResult(CuratorTransactionResult result) {
            this.document.setVersion(0);
        }

        @Override
        public OperationType getOperationType() {
            return OperationType.CREATE;
        }
    }

    private static class SetOperation extends Operation {
        private Document document;
        private byte[] data;

        SetOperation(Path path, Document document) {
            super(path);
            this.document = document;
            this.data = document.getData().getBytes();
        }

        @Override
        public CuratorTransactionBridge getCuratorEquivalent(CuratorTransaction transaction) throws Exception {
            return transaction.setData().withVersion(document.getVersion()).forPath(path.toString(), data);
        }

        @Override
        public CuratorTransactionBridge getCuratorEquivalent(CuratorTransactionBridge bridge) throws Exception {
            return bridge.and().setData().withVersion(document.getVersion()).forPath(path.toString(), data);
        }

        @Override
        public void updateWithResult(CuratorTransactionResult result) {
            this.document.setVersion(result.getResultStat().getVersion());
        }

        @Override
        public OperationType getOperationType() {
            return OperationType.SET_DATA;
        }
    }

    public CamilleTransaction() {
        CuratorFramework curator = CamilleEnvironment.getCamille().getCuratorClient();
        transaction = curator.inTransaction();
        operations = new ArrayList<Operation>();
    }

    /**
     * Performs a transactional check, which asserts that the provided document
     * is up-to-date relative to what's in the repository.
     */
    public void check(Path path, Document document) {
        operations.add(new CheckOperation(path, document));
    }

    /**
     * Performs a transactional create. This creates the provided document with
     * the provided ACL at the provided path within a transactional context.
     */
    public void create(Path path, Document document, List<ACL> acl) {
        operations.add(new CreateOperation(path, document, acl));
    }

    public void create(Path path, List<ACL> acl) {
        operations.add(new CreateOperation(path, new Document(), acl));
    }

    /**
     * Performs a transactional set operation. All set operations send document
     * versions and so will result in an exception if the version in the
     * repository doesn't match the version of the document.
     */
    public void set(Path path, Document document) {
        operations.add(new SetOperation(path, document));
    }

    /**
     * Performs a transactional delete operation. Does not care about document
     * versions.
     */
    public void delete(Path path) {
        operations.add(new DeleteOperation(path));
    }

    /**
     * Commits the pending operations. Note that as a side-effect, upon success,
     * document versions will be updated to reflect what's in the repository.
     */
    public void commit() throws Exception {
        CuratorTransactionBridge built = null;
        for (Operation operation : operations) {
            if (built == null) {
                built = operation.getCuratorEquivalent(transaction);
            } else {
                built = operation.getCuratorEquivalent(built);
            }
        }

        Collection<CuratorTransactionResult> results = built.and().commit();
        for (CuratorTransactionResult result : results) {
            Operation operation = lookupOperation(result.getType(), new Path(result.getForPath()));
            operation.updateWithResult(result);
        }
    }

    private Operation lookupOperation(OperationType type, Path path) {
        for (Operation operation : operations) {
            if (operation.getOperationType() == type && operation.getPath().equals(path)) {
                return operation;
            }
        }
        return null;
    }
}
