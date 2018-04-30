angular.module('lp.sfdc.credentials', [])
.component('salesforceSettings', {
    templateUrl: 'app/sfdc/sfdcsettings.component.html',
    bindings: {
        orgs: '=',
        accountids: '='
    },
    controller: function(
        $scope, $state, $timeout, 
        ResourceUtility, BrowserStorageUtility, sfdcservice
    ) {
        var vm = this;

        vm.$onInit = function() {
            console.log(vm.orgs);
            console.log(vm.accountids);
        }

        vm.originalData = angular.copy(vm.orgs);

        vm.uiCanExit = function() {
          if (!angular.equals(vm.orgs, vm.originalData)) {
            return window.confirm("Data has changed.  Exit anyway and lose changes?");
          }
        }

        vm.generateAndEmailSFDCAccessTokenClicked = function() {
            var clientSession = BrowserStorageUtility.getClientSession(),
                emailAddress = clientSession.EmailAddress,
                tenantId = clientSession.Tenant.Identifier;

            sfdcservice.generateAuthToken(emailAddress, tenantId).then(function (result) {
                console.log(result);
            });
        };

        vm.saveOrgs = function() {
            var orgs = vm.orgs;
            angular.forEach(orgs, function(value, key) {
                sfdcservice.saveOrgs(value.configId, value).then(function(result){
                    console.log(result);
                });
            });
        }
    }
});