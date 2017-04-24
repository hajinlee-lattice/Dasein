angular.module('lp.import')
.service('ImportWizardStore', function($q, ImportWizardService){
    var ImportWizardStore = this;

    this.accountIdState = {
        accountDedupeField: null,
        dedupeType: 'custom',
        selectedField: null,
        fields: ['Id']
    };

    this.getAccountIdState = function() {
        return this.accountIdState;
    };

    this.setAccountIdState = function(nextState) {
        for (var key in this.accountIdState) {
            this.accountIdState[key] = nextState[key];
        }
    };
})
.service('ImportWizardService', function($q, $http, $state) {
});
