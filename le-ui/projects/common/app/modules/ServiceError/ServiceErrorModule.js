angular
.module('mainApp.core.modules.ServiceErrorModule', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ServiceErrorInterceptor', function ($q, $injector) {
    this.response = function(response) {
        //console.log('response', response.status, response);
        var ServiceErrorUtility = $injector.get('ServiceErrorUtility');
        ServiceErrorUtility.process(response);

        return response || $q.when(response);
    };

    this.request = function(response) {
        //console.log('request', response.status, response);
        var ServiceErrorUtility = $injector.get('ServiceErrorUtility');
        ServiceErrorUtility.process(response);

        return response || $q.when(response);
    };

    this.responseError = function(rejection) {
        //console.log('responseError', rejection);
        var ServiceErrorUtility = $injector.get('ServiceErrorUtility');
        ServiceErrorUtility.process(rejection);

        return $q.reject(rejection);
    };

    this.requestError = function(rejection) {
        //console.log('requestError', rejection);
        var ServiceErrorUtility = $injector.get('ServiceErrorUtility');
        ServiceErrorUtility.process(rejection);

        return $q.reject(rejection);
    };
})
.config(function ($httpProvider) {
    $httpProvider.interceptors.push('ServiceErrorInterceptor');
})
.service('ServiceErrorUtility', function ($compile, $templateCache, $http, $rootScope) {
    this.check = function (response) {
        //console.log('check', response);
        return (response && response.data && (response.data.error || response.data.error_description || response.data.errorMsg || response.data.errorMsg));
    };

    this.process = function (response) {
        //console.log('process', response);
        if (this.check(response)) {
            var config = response.config || { headers: {} },
                params = (config.headers.ErrorDisplayMethod || 'banner').split('|'),
                method = params[0],
                state = params[1] || null, // state or elementQuery
                stateParams = params[2] || null;
            
            switch (method) {
                case 'none': break;
                case 'popup': this.showModal(response, false, state, stateParams); break;
                case 'modal': this.showModal(response, true, state, stateParams); break;
                case 'banner': this.showBanner(response, state); break;
                case 'suppress': this.showSuppressed(response); break;
                default: this.showModal(response);
            }
        }
    };

    this.hideBanner = function (elementQuery) {
        $(elementQuery || "#mainInfoView").html('');
    };

    this.showBanner = function (response, elementQuery) {
        if (!this.check(response)) {
            return;
        }

        $http.get('/app/modules/ServiceError/ServiceErrorBanner.html', { cache: $templateCache }).success(function (html) {
            var scope = $rootScope.$new(),
                data = response.data;

            scope.errorCode = data.errorCode || data.error;
            scope.errorMsg = data.errorMsg || data.error_description;
            scope.status = response.status;
            scope.statusText = response.statusText;

            $compile($(elementQuery || "#mainInfoView").html(html))(scope);
        });
    };

    this.showModal = function (response, isModal, state, stateParams) {
        if (!this.check(response)) {
            return;
        }
        
        $http.get('/app/modules/ServiceError/ServiceErrorModal.html', { cache: $templateCache }).success(function (html) {
            var modalElement = $("#modalContainer"),
                scope = $rootScope.$new(),
                data = response.data,
                options = {
                    backdrop: "static"
                };

            scope.errorCode = data.errorCode;
            scope.errorMsg = data.errorMsg;
            scope.status = response.status;
            scope.state = state;
            scope.stateParams = stateParams;
            scope.statusText = response.statusText;

            $compile(modalElement.html(html))(scope);

            modalElement.modal(options);
            modalElement.modal('show');
            
            // Remove the created HTML from the DOM
            modalElement.on('hidden.bs.modal', function (event) {
                modalElement.empty();
            });

            scope.modalElement = modalElement;
        });
    };
})
.controller('ServiceErrorController', function ($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
});
