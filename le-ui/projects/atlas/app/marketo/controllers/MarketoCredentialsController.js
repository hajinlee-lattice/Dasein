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
		latticeSecretKey: '',
		state: 'create',
		saveInProgress: false,
    	addCredentialErrorMessage: "",
    	showAddCredentialError: false
    });

	vm.createCredentialClicked = function() {
		
		vm.saveInProgress = true;

		var credential = {
            credentialName: vm.credentialName,
            soapEndpoint: vm.soapEndpoint,
            soapUserId: vm.soapUserId,
            soapEncryptionKey: vm.soapEncryptionKey,
            restEndpoint: vm.restEndpoint,
            restIdentityEndpoint: vm.restIdentityEndpoint,
            restClientId: vm.restClientId,
            restClientSecret: vm.restClientSecret,
            latticeSecretKey: vm.latticeSecretKey
		};

		MarketoService.CreateMarketoCredential(credential).then(function(result){

			if (result != null && result.success === true) {
				$state.go('home.marketosettings.apikey', {}, { reload: true });
			} else {
				vm.saveInProgress = false;
				vm.addCredentialErrorMessage = result;
				vm.showAddCredentialError = true;
			}

		});

	};

	vm.onBlur = function($event){

		var desiredCredentialName = vm.credentialName;


		// var id = arr.length + 1;
		// var found = arr.some(function (el) {
		// 	return el.username === name;
		// });
		// if (!found) { arr.push({ id: id, username: name }); }
		console.log(vm.credentials);

        _.each(vm.credentials, function(value, key){

        	vm.credential = value;

			if(value.value == desiredCredentialName){
				return true;
			} else {
				return false;
			}

	   	});

	};

	vm.cancelCredentialCreate = function(){
		$state.go('home.marketosettings.apikey');		
	};
	
}])
.controller('MarketoCredentialsEditController', ['FeatureFlags','MarketoCredential', 'MarketoService', '$state', '$stateParams', function(FeatureFlags, MarketoCredential, MarketoService, $state, $stateParams) {

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
		latticeSecretKey: MarketoCredential.lattice_secret_key,
		state: 'edit',
		saveInProgress: false,
    	addUserErrorMessage: "",
    	showAddUserError: false,
    	showSecretKey: FeatureFlags.LatticeMarketoScoring && MarketoCredential.lattice_secret_key
    });

	vm.saveCredentialClicked = function() {

		vm.saveInProgress = true;

		var credential = {
            credentialName: vm.credentialName,
            soapEndpoint: vm.soapEndpoint,
            soapUserId: vm.soapUserId,
            soapEncryptionKey: vm.soapEncryptionKey,
            restEndpoint: vm.restEndpoint,
            restIdentityEndpoint: vm.restIdentityEndpoint,
            restClientId: vm.restClientId,
            restClientSecret: vm.restClientSecret,
            latticeSecretKey: vm.latticeSecretKey
		};

		MarketoService.UpdateMarketoCredential(vm.credentialId, credential).then(function(result) {
			
			var errorMsg = result.errorMsg;

			if (result.success) {
				$state.go('home.marketosettings.apikey');
			} else {
				vm.saveInProgress = false;
				vm.addCredentialErrorMessage = errorMsg;
				vm.showAddCredentialError = true;
			}
		});
	}

	vm.cancelCredentialCreate = function(){
		$state.go('home.marketosettings.apikey');
	};

}])
.controller('MarketoCredentialsController', ['MarketoCredentials', 'MarketoService', 'DeleteCredentialModal', 'ResourceUtility', function(MarketoCredentials, MarketoService, DeleteCredentialModal, ResourceUtility) {
    var vm = this;

    angular.extend(vm, {
    	ResourceUtility: ResourceUtility,
		credentials: MarketoCredentials
    });

	vm.deleteCredentialClicked = function(credentialId) {
        DeleteCredentialModal.show(credentialId);
	}

}]);
