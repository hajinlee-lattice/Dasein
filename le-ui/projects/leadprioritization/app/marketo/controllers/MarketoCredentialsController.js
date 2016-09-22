angular.module('lp.marketo', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('MarketoCredentialSetupController', ['$scope', '$state', '$stateParams', 'BrowserStorageUtility', 'ResourceUtility', 'MarketoService',
	function($scope, $state, $stateParams, BrowserStorageUtility, ResourceUtility, MarketoService){

    var vm = this;

    angular.extend(vm, {
		credentialName: '',
		soapEndpoint: '',
		soapUserId: '',
		soapEncryptionKey: '',
		restEndpoint: '',
		restIdentityEndpoint: '',
		restClientId: '',
		restClientSecret: ''
    });
    // $scope.credentialIsSetup = false;
    // $scope.credentials = MarketoCredentials.resultObj;

	vm.createCredentialClicked = function() {
		var credential = {
            credentialName: vm.credentialName,
            soapEndpoint: vm.soapEndpoint,
            soapUserId: vm.soapUserId,
            soapEncryptionKey: vm.soapEncryptionKey,
            restEndpoint: vm.restEndpoint,
            restIdentityEndpoint: vm.restIdentityEndpoint,
            restClientId: vm.restClientId,
            restClientSecret: vm.restClientSecret
		};

		MarketoService.CreateMarketoCredential(credential);
	};

	// $scope.editCredentialClick = function($event){
		
	// }

	// $scope.setupCredentialClick = function($event){
		
	// }

	// $scope.deleteCredentialClick = function($event){

	// }
	
}])
.controller('MarketoCredentialsController', ['MarketoCredentials', function(MarketoCredentials) {
    var vm = this;

    angular.extend(vm, {
		credentials: MarketoCredentials
    });
}]);