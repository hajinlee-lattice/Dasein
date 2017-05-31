angular.module('lp.playbook')
.service('PlaybookWizardStore', function($q, PlaybookWizardService){
    var PlaybookWizardStore = this;

    this.savedSegment = this.savedSegment || {};

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

    this.saveSegment = function(segment) {
        if(segment) {
            this.savedSegment = segment;
        }
    }
})
.service('PlaybookWizardService', function($q, $http, $state) {
});
