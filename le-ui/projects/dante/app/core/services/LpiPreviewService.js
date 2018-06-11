angular.module('mainApp.core.services.LpiPreviewService',[])
.service('LpiPreviewStore', function() {
    this.lead = null;

    this.setLead = function(lead){
        this.lead = lead;
    };

    this.getLead = function() {
        return this.lead;
    };
});
