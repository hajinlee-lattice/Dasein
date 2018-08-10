angular
.module('common.exceptions', [
    'common.banner',
    'common.modal',
    'common.notice',
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
.service('ServiceErrorUtility', function($timeout, Banner, Modal, Notice) {
    this.check = function (response) {
        if (!response || !response.data) {
            return false;
        }

        var data = response.data;
        var uiErrorCheck = !!(data.error || data.error_description || data.errorMsg);
        var uiActionCheck = !!(data.UIAction);

        //console.log('-!- exceptions check:', uiErrorCheck, uiActionCheck, data);
        return uiErrorCheck || uiActionCheck;
    };

    this.process = function (response) {
        if (this.check(response)) {
            //console.log('-!- exceptions process:', response);
            var config = response.config || { headers: {} },
                params = (config.headers.ErrorDisplayMethod || 'banner').split('|'),
                payload = response.data,
                uiAction = payload ? payload.UIAction : {},
                method = (uiAction ? uiAction.view : params[0]).toLowerCase();
            
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

        var payload = response.data,
            uiAction = payload.UIAction || {},
            type = (uiAction.status || 'error').toLowerCase(),
            http_err = response.statusText,
            http_code = response.status,
            url = response.config.url,
            title = uiAction.title || (http_code + ' "' + http_err + '" ' + url),
            message = uiAction.message || payload.errorMsg || payload.error_description;

        //console.log('-!- exceptions show:', type, title, message, Service);
        $timeout(function() {
            Service[type]({ title: title, message: message });
        }, 1);
    };

    this.hideBanner = function() {
        Banner.reset();
    };
})
.controller('ServiceErrorController', function($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
});
