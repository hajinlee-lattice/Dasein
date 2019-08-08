angular.module('lp.sfdc.credentials', ['ngAnimate', 'lp.sfdc', 'common.modal', 'common.utilities.browserstorage'])
.component('sales', {
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
            if (!vm.featureflags.EnableCdl) {
                $state.go('.', {pageTitle: 'Salesforce Settings'}, {notify: false});
            } else {
                $state.go('.', {pageTitle: 'Application Settings'}, {notify: false});
            }

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
                // console.log(result);
                if (result.Success == true) {
                    if (vm.cdlIsEnabled) {
                        Notice.success({
                            delay: 5000,
                            title: 'Email sent to ' + emailAddress, 
                            message: 'Your one-time authentication token has been sent to your email.'
                        });
                    } else {
                        Notice.success({
                            delay: 5000,
                            message: 'We have sent you an email with an access token and instructions for authenticating with Salesforce.'
                        });
                    }
                } else {
                    if (vm.cdlIsEnabled) {
                        Banner.error({message: result.Errors[0]});
                    } else {
                        Banner.error({message: 'Failed to Generate Salesforce Access Token.'});
                    }
                    
                }



                
            });
        };

        vm.closeStatusMessage = function() {
            vm.showSuccess = false;
        };
    }
});