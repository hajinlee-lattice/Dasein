angular
.module('mainApp.core.modals.ServiceErrorModal', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.factory('ServiceErrorModalInterceptor', function ($q, $injector) {
    return {
        response: function(response) {
            console.log('response', response.status, response);
            var ServiceErrorModalUtility = $injector.get('ServiceErrorModalUtility');
            ServiceErrorModalUtility.check(response);

            return response || $q.when(response);
        },
        responseError: function(rejection) {
            console.log('responseError', rejection.status, rejection);
            var ServiceErrorModalUtility = $injector.get('ServiceErrorModalUtility');
            ServiceErrorModalUtility.check(rejection);

            return $q.reject(rejection);
        }
    };
})
.config(function ($httpProvider) {
    $httpProvider.interceptors.push('ServiceErrorModalInterceptor');
})
.service('ServiceErrorModalUtility', function ($compile, $templateCache, $http, $rootScope) {
    this.check = function (response) {
        if (response && response.data && (response.data.errorCode || response.data.errorMsg)) {
            this.show(response)
        }
    };

    this.show = function (response) {
        $http.get('/app/views/ServiceErrorModalView.html', { cache: $templateCache }).success(function (html) {
            var modalElement = $("#modalContainer"),
                scope = $rootScope.$new(),
                data = response.data,
                options = {
                    backdrop: "static"
                };

            scope.errorCode = data.errorCode;
            scope.errorMsg = data.errorMsg;
            scope.status = response.status;
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
.controller('ServiceErrorModalController', function ($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
});
