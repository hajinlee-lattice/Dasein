package com.latticeengines.domain.exposed.cdl.sla;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.ActionType;

public class ActionPredicate implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty("action_type")
    private ActionType actionType;

    //list item 'and' logic
    @JsonProperty("action_config_predicates")
    private List<ActionConfigPredicate> actionConfigPredicates = new ArrayList<>();

    public ActionType getActionType() {
        return actionType;
    }

    public void setActionType(ActionType actionType) {
        this.actionType = actionType;
    }

    public List<ActionConfigPredicate> getActionConfigPredicates() {
        return actionConfigPredicates;
    }

    public void setActionConfigPredicates(List<ActionConfigPredicate> actionConfigPredicates) {
        this.actionConfigPredicates = actionConfigPredicates;
    }

    public void addActionConfigPredicate(ActionConfigPredicate actionConfigPredicate) {
        this.actionConfigPredicates.add(actionConfigPredicate);
    }

    public void removeActionConfigPredicate(ActionConfigPredicate actionConfigPredicate) {
        this.actionConfigPredicates.remove(actionConfigPredicate);
    }
}
