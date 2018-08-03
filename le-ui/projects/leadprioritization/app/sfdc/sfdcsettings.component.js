angular.module('lp.sfdc.credentials', ['ngAnimate'])
.component('salesforceSettings', {
    templateUrl: 'app/sfdc/sfdcsettings.component.html',
    bindings: {
        featureflags: '<',
        orgs: '<',
        accountids: '<',
        externaltypes: '<'
    },
    controller: function(
        $q, $scope, $state, $timeout, 
        ResourceUtility, BrowserStorageUtility, SfdcService, Modal, SfdcStore, Banner, Notice
    ) {
        var vm = this;

        vm.initModalWindow = function () {  
            vm.config = {
                'name': "leave-with-unsaved-changes",
                'type': 'md',
                'title': 'Save before leaving?',
                'titlelength': 100,
                'dischargetext': 'CANCEL (go back & save)',
                'dischargeaction' :'cancel',
                'confirmtext': 'Yes, proceed to another page.',
                'confirmaction' : 'ok',
                'icon': 'fa fa-exclamation-triangle',
                'iconstyle': {'color': 'white'},
                'confirmcolor': 'blue-button',
                'showclose': true,
                'headerconfig': {'background-color':'#FDC151', 'color':'white'},
                'confirmstyle' : {'background-color':'#FDC151'}
            };
        
            vm.toggleModal = function () {
                var modal = Modal.get(vm.config.name);
                if(modal){
                    modal.toggle();
                }
            }

            $scope.$on("$destroy", function() {
                Modal.remove(vm.config.name);
            });
        }

        vm.$onInit = function() {
            vm.accountIDMap = {};
            var ids = [];
            vm.externaltypes.forEach(function(type) {
                if (vm.accountids[type] != undefined) {
                    vm.accountids[type].forEach(function(account) {
                        vm.accountIDMap[account.fieldName] = type;
                        ids.push(account);
                    })
                }
            });
            vm.accountids = ids;

            vm.cdlIsEnabled = vm.featureflags.EnableCdl;
            vm.generateAuthTokenButtonLabel = vm.cdlIsEnabled ? 'Email One-time Authentication Token' : 'Generate Salesforce Access Token';

            vm.initModalWindow();
        }

        vm.originalData = angular.copy(vm.orgs);

        vm.uiCanExit = function() {

            vm.showSuccess = false;

          var deferred = $q.defer();

          if (!angular.equals(vm.orgs, vm.originalData)) {
            
            vm.toggleModal();
            vm.modalCallback = function (args) {
                if(vm.config.dischargeaction === args.action){
                    vm.toggleModal();
                    deferred.resolve(false);
                } else if(vm.config.confirmaction === args.action){
                    vm.toggleModal();
                    deferred.resolve(true);
                }
            }
          } else {
            deferred.resolve(true);
          }
          return deferred.promise;
        }

        vm.generateAuthToken = function() {
            var clientSession = BrowserStorageUtility.getClientSession(),
                emailAddress = clientSession.EmailAddress,
                tenantId = clientSession.Tenant.Identifier;

            SfdcService.generateAuthToken(emailAddress, tenantId).then(function (result) {
                Banner.success({title: 'Email sent to ' + emailAddress, message: 'After you authenticate a new system org, a new ID will be listed below.'});

                $timeout(function(){
                    Banner.get().shift();
                }, 5000);

                
            });
        };

        vm.saveOrgs = function() {
            var orgs = vm.orgs;
            angular.forEach(orgs, function(value, key) {
                SfdcService.saveOrgs(value.configId, value).then(function(result){
                    console.log(result);
                    Notice.success({message: 'Your changes have been saved.'});
                });
            });

            SfdcStore.setOrgs(vm.orgs);
            vm.originalData = angular.copy(vm.orgs);
        };

        vm.saveOrg = function(org) {
            if (org.accountId != '') { // FIXME - ng-change is called twice when setting accountId to blank option 
                SfdcService.saveOrgs(org.configId, org).then(function(result){
                    console.log(result);
                    Notice.success({message: 'Your changes have been saved.'});
                });
                SfdcStore.setOrgs(vm.orgs);
                vm.originalData = angular.copy(vm.orgs);
            }
        };

        vm.closeStatusMessage = function() {
            vm.showSuccess = false;
        };
    }
});