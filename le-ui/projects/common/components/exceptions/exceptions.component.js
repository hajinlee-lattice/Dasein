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
.service('ServiceErrorUtility', function(Banner, Modal, Notice) {
    this.check = function (response) {
        return (response && response.data && (response.data.error || response.data.error_description || response.data.errorMsg || response.data.errorMsg));
    };

    this.process = function (response) {
        if (this.check(response)) {
            var config = response.config || { headers: {} },
                params = (config.headers.ErrorDisplayMethod || 'banner').split('|'),
                method = params[0];
            
            switch (method) {
                case 'none': break;
                case 'popup': this.show(Modal, response); break;
                case 'modal': this.show(Modal, response); break;
                case 'banner': this.show(Banner, response); break;
                case 'notice': this.show(Notice, response); break;
                case 'suppress': console.error(response); break;
                default: this.show(Modal, response);
            }
        }
    };

    this.show = function(Service, response) {
        if (!this.check(response)) {
            return;
        }

        var data = response.data,
            http_err = response.statusText,
            http_code = response.status,
            ledp_code = data.errorCode || data.error,
            desc = data.errorMsg || data.error_description,
            url = response.config.url;

        Service.error({
            title: http_code + ' ' + http_err + ': ' + url,
            message: desc + ' (' + ledp_code + ')',
        });
    };

    this.hideBanner = function() {
        Banner.reset();
    };
})
.controller('ServiceErrorController', function ($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
});
