angular.module('lp.marketo', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.marketo.modals.DeleteCredentialModal'
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
		restClientSecret: '',
		state: 'create'
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

	vm.cancelCredentialCreate = function(){
		$state.go('home.marketosettings.apikey');		
	};
	
}])
.controller('MarketoCredentialsEditController', ['MarketoCredential', 'MarketoService', '$state', '$stateParams', function(MarketoCredential, MarketoService, $state, $stateParams) {

    var vm = this;
    angular.extend(vm, {
		credential: MarketoCredential,
    	credentialId: $stateParams.id,
		credentialName: MarketoCredential.name,
		soapEndpoint: MarketoCredential.soap_endpoint,
		soapUserId: MarketoCredential.soap_user_id,
		soapEncryptionKey: MarketoCredential.soap_encryption_key,
		restEndpoint: MarketoCredential.rest_endpoint,
		restIdentityEndpoint: MarketoCredential.rest_identity_endpoint,
		restClientId: MarketoCredential.rest_client_id,
		restClientSecret: MarketoCredential.rest_client_secret,
		state: 'edit'
    });

	vm.saveCredentialClicked = function() {
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

		MarketoService.UpdateMarketoCredential(vm.credentialId, credential).then(function(result) {
			if (result.success) {
				$state.go('home.marketosettings.apikey');
			} else {

			}
		});
	}

	vm.cancelCredentialCreate = function(){
		$state.go('home.marketosettings.apikey');
	};

}])
.controller('MarketoCredentialsController', ['MarketoCredentials', 'MarketoService', 'DeleteCredentialModal', function(MarketoCredentials, MarketoService, DeleteCredentialModal) {
    var vm = this;

    angular.extend(vm, {
		credentials: MarketoCredentials
    });

	vm.deleteCredentialClicked = function(credentialId) {
        DeleteCredentialModal.show(credentialId);
	}

	vm.init = function() {
        _.each(vm.credentials, function(value, key){

        	vm.credential = value;

			if(value.enrichment.marketo_match_fields[0].marketoFieldName == null){
				vm.credentialIsSetup = false;
			} else {
				vm.credentialIsSetup = true;
			}

	   	});
    }

    vm.init();

}]);