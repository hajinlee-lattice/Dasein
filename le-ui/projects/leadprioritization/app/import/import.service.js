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

    this.getCustomFields = function(type) {
        var data = [],
            total = Math.floor(Math.random() * 10 + 1),
            types = ['Text', 'Number', 'Boolean', 'Date'];
        for(var i=0;i<total;i++) {
            var tmp = {
                CustomField: 'CustomField' + (i + 1),
                Type: types, //[Math.floor(Math.random()*types.length)],
                Ignore: false //Math.random() >= 0.5
            };
            data.push(tmp);
        }
        return data;
    }
})
.service('ImportWizardService', function($q, $http, $state) {
});
