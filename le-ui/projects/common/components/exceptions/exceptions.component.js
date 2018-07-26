angular
.module('mainApp.core.modules.ServiceErrorModule', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ServiceErrorInterceptor', function ($q, $injector) {
    this.response = function(response) {
        var ServiceErrorUtility = $injector.get('ServiceErrorUtility');
        ServiceErrorUtility.process(response);

        return response || $q.when(response);
    };

    this.request = function(response) {
        var ServiceErrorUtility = $injector.get('ServiceErrorUtility');
        ServiceErrorUtility.process(response);

        return response || $q.when(response);
    };

    this.responseError = function(rejection) {
        var ServiceErrorUtility = $injector.get('ServiceErrorUtility');
        ServiceErrorUtility.process(rejection);

        return $q.reject(rejection);
    };

    this.requestError = function(rejection) {
        var ServiceErrorUtility = $injector.get('ServiceErrorUtility');
        ServiceErrorUtility.process(rejection);

        return $q.reject(rejection);
    };
})
.config(function ($httpProvider) {
    $httpProvider.interceptors.push('ServiceErrorInterceptor');
})
.service('ServiceErrorUtility', function(Banner, Modal) {
    this.check = function (response) {
        return (response && response.data && (response.data.error || response.data.error_description || response.data.errorMsg || response.data.errorMsg));
    };

    this.process = function (response) {
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

    this.showBanner = function (response, elementQuery) {
        if (!this.check(response)) {
            return;
        }

        var data = response.data,
            http_err = response.statusText,
            http_code = response.status,
            ledp_code = data.errorCode || data.error,
            desc = data.errorMsg || data.error_description,
            url = response.config.url;

        Banner.error({
            title: http_code + ' ' + http_err + ': ' + url,
            message: desc + ' (' + ledp_code + ')',
        });
    };

    this.showModal = function (response, isModal, state, stateParams) {
        if (!this.check(response)) {
            return;
        }

        var data = response.data,
            http_err = response.statusText,
            http_code = response.status,
            ledp_code = data.errorCode || data.error,
            desc = data.errorMsg || data.error_description,
            url = response.config.url;

        Modal.error({
            type: 'md',
            title: http_err + ': ' + url,
            message: desc + ' (' + ledp_code + ')',
        });
    };
})
.controller('ServiceErrorController', function ($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
});
