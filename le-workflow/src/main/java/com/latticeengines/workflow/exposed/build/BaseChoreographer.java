package com.latticeengines.workflow.exposed.build;

import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class BaseChoreographer implements Choreographer {

    protected static final String ROOT = "root";

    private List<List<String>> stepNamespaces;

    @Override
    public boolean skipStep(AbstractStep<? extends BaseStepConfiguration> step, int seq) {
        return step.getConfiguration() != null && step.getConfiguration().isSkipStep();
    }

    @Override
    public void linkStepNamespaces(List<List<String>> stepNamespaces) {
        this.stepNamespaces = stepNamespaces;
    }

    protected List<String> getStepNamespace(int seq) {
        try {
            return stepNamespaces.get(seq);
        } catch (IndexOutOfBoundsException e) {
            return Collections.emptyList();
        }
    }

    protected String getParentWorkflow(int seq) {
        List<String> namespace = getStepNamespace(seq);
        if (namespace.isEmpty()) {
            return ROOT;
        } else {
            return namespace.get(namespace.size() - 1);
        }
    }

    protected boolean namespaceBeginWith(int seq, List<String> namespace) {
        List<String> stepNamespace = getStepNamespace(seq);
        if (CollectionUtils.isNotEmpty(namespace) && stepNamespace.size() >= namespace.size()) {
            List<String> prefix = stepNamespace.subList(0, namespace.size());
            List<String> lcs = ListUtils.longestCommonSubsequence(prefix, namespace);
            return lcs.size() == namespace.size();
        }
        return false;
    }

    protected boolean namespaceEndWith(int seq, List<String> namespace) {
        List<String> stepNamespace = getStepNamespace(seq);
        if (CollectionUtils.isNotEmpty(namespace) && stepNamespace.size() >= namespace.size()) {
            List<String> suffix = stepNamespace.subList(stepNamespace.size() - namespace.size(), stepNamespace.size());
            List<String> lcs = ListUtils.longestCommonSubsequence(suffix, namespace);
            return lcs.size() == namespace.size();
        }
        return false;
    }

    protected boolean namespaceContains(int seq, List<String> subNamespace) {
        List<String> stepNamespace = getStepNamespace(seq);
        if (CollectionUtils.isNotEmpty(subNamespace) && stepNamespace.size() >= subNamespace.size()) {
            List<String> lcs = ListUtils.longestCommonSubsequence(stepNamespace, subNamespace);
            return lcs.size() == subNamespace.size();
        }
        return false;
    }

}
