angular.module('lp.playbook')
.service('PlaybookWizardStore', function($q, PlaybookWizardService){
    var PlaybookWizardStore = this;

    this.getSegments = function() {
        var data = [],
            total = Math.floor(Math.random() * 10 + 3); // 3 - 10

        for(var i=0;i<total;i++) {
            var tmp = {
                Name: 'Segment Name ' + (i + 1),
                Accounts: Math.floor(Math.random() * (100*1000) + 1),
                Contacts: Math.floor(Math.random() * (100*1000) + 1),
                Lift: Math.random() * 10 + 1
            };
            data.push(tmp);
        }
        return data;
    };

    this.getRatings = function() {
        var data = [],
            total = Math.floor(Math.random() * 10 + 3); // 3 - 10

        for(var i=0;i<total;i++) {
            var tmp = {
                Name: 'Rating Name ' + (i + 1)
            };
            data.push(tmp);
        }
        return data;
    };
})
.service('PlaybookWizardService', function($q, $http, $state) {
});
