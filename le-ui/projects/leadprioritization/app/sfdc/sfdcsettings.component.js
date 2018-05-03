angular.module('lp.sfdc.credentials', ['ngAnimate'])
.component('salesforceSettings', {
    templateUrl: 'app/sfdc/sfdcsettings.component.html',
    bindings: {
        orgs: '<',
        accountids: '<'
    },
    controller: function(
        $scope, $state, $timeout, 
        ResourceUtility, BrowserStorageUtility, SfdcService, ModalStore
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
        
            vm.modalCallback = function (args) {
                if('closedForced' === args) {
                }else if(vm.config.dischargeaction === args){
                    vm.toggleModal();
                } else if(vm.config.confirmaction === args){
                    var modal = ModalStore.get(vm.config.name);
                    modal.waiting(true);
                    modal.disableDischargeButton(true);
                }
            }
            vm.toggleModal = function () {
                var modal = ModalStore.get(vm.config.name);
                if(modal){
                    modal.toggle();
                }
            }

            $scope.$on("$destroy", function() {
                ModalStore.remove(vm.config.name);
            });
        }

        vm.$onInit = function() {
            console.log(vm.orgs);
            console.log(vm.accountids);

            vm.initModalWindow();
        }

        vm.originalData = angular.copy(vm.orgs);

        vm.uiCanExit = function() {
          if (!angular.equals(vm.orgs, vm.originalData)) {
            return window.confirm("Data has changed.  Exit anyway and lose changes?");
            // vm.toggleModal();
          }
        }

        vm.generateAndEmailSFDCAccessTokenClicked = function() {
            var clientSession = BrowserStorageUtility.getClientSession(),
                emailAddress = clientSession.EmailAddress,
                tenantId = clientSession.Tenant.Identifier;

            SfdcService.generateAuthToken(emailAddress, tenantId).then(function (result) {
                vm.showSuccess = true;
                vm.successMessage = 'Your one-time authentication token has been sent to your email.';
                console.log(result);
                $timeout(function(){
                    vm.showSuccess = false;
                }, 3000);
            });
        };

        vm.saveOrgs = function() {
            var orgs = vm.orgs;
            angular.forEach(orgs, function(value, key) {
                SfdcService.saveOrgs(value.configId, value).then(function(result){
                    console.log(result);
                    vm.showSuccess = true;
                    vm.successMessage = 'Your changes have been saved.';
                    $timeout(function(){
                        vm.showSuccess = false;
                    }, 3000);
                });
            });
        };

        vm.closeStatusMessage = function() {
            vm.showSuccess = false;
        };
    }
});