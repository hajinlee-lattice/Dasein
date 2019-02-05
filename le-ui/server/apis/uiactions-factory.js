const UIActionsFactory = {
    getUIActionsObject(title, view, status) {
        return new UIActions(title, view, status);
        // {"UIAction":{"title":"Segment Test Segment is safe to edit","view":"Notice","status":"Success"}}
    }
};

module.exports = UIActionsFactory;

class UIActions {
    constructor(title='Tile', view='Notice', status="Success"){
        this.UIAction = {};
        this.UIAction['title'] = title;
        this.UIAction['view'] = view;
        this.UIAction['status'] = status;
    }
}
