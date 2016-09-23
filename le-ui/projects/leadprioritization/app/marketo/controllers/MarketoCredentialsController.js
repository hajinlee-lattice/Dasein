angular.module('lp.marketo', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility'
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

	vm.cancelCredentialCreate = function(){
		$state.go('home.marketosettings.apikey');		
	};
	
}])
.controller('MarketoCredentialsController', ['MarketoCredentials', function(MarketoCredentials) {
    var vm = this;

    angular.extend(vm, {
		credentials: MarketoCredentials
    });

    vm.setupCredentialClick = function($event){
		$rootScope.$broadcast(NavUtility.MODEL_DETAIL_NAV_EVENT, data);
	}

	vm.editCredentialClick = function($event){
		console.log("edit credential");	
	}

	vm.deleteCredentialClick = function($event){
		MarketoService.DeleteMarketoCredential(credentials.id);
	}


	vm.init = function() {
        _.each(vm.credentials, function(value, key){
			if(value.enrichment.marketo_match_fields[0].marketoFieldName == null){
				vm.credentialIsSetup = false;
			} else {
				vm.credentialIsSetup = true;
			}
	   	});
    }

    vm.init();



}]);