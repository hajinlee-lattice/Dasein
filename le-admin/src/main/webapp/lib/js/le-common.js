(function(){
    angular.module('le.common.directives.ngEnterDirective', [])
        .directive('ngEnter', function () {
            return function (scope, element, attrs) {
                element.bind("keydown keypress", function (event) {
                    if (event.which === 13) {
                        scope.$apply(function () {
                            scope.$eval(attrs.ngEnter);
                        });

                        event.preventDefault();
                    }
                });
            };
        });
}).call();

(function(){
    angular.module('le.common.directives.ngQtipDirective', [])
        .directive('ngQtip', function () {
            return {
                restrict: 'A',
                link: function(scope, element, attrs) {
                    return $(element).qtip({
                        content: attrs.title,
                        style: 'qtip-bootstrap'
                    });
                }
            };
        });
}).call();
(function(){
    angular.module('le.common.directives.helperMarkDirective', [
        'le.common.directives.ngQtipDirective'
    ])
    .directive('helperMark', function () {
        return {
            restrict: 'E',
            scope: {help: '@'},
            template: '<span class="has-tooltip" ng-qtip title="{{help}}"><i class="fa fa-question-circle"></i></span>'
        };
    });
}).call();

(function(){
    var app = angular.module('le.common.util.UnderscoreUtility', [
    ]);

    app.factory('_', function() {
        return window._; // assumes underscore has already been loaded on the page
    });
}).call();

angular.module('le.common.util.BrowserStorageUtility', ['LocalStorageModule'])
.service('BrowserStorageUtility', function(localStorageService) {
        var storageKey = "storage";

        var getStorage = function(){
            return localStorageService.get(storageKey);
        };

        var setStorage = function(data){
            localStorageService.set(storageKey, data);
        };

        var clearStorage = function(){
            localStorageService.remove(storageKey);
        };

        this._tokenDocumentStorageKey = "Token";
        this._loginDocumentStorageKey = "LoginDocument";
        this._sessionDocumentStorageKey = "SessionDocument";

        this.setTokenDocument = function (data, successHandler) {
            this._setProperty(data, successHandler, "_tokenDocumentStorageKey");
        };

        this.getTokenDocument = function () {
            return this._getProperty("_tokenDocumentStorageKey");
        };

        this.setLoginDocument = function (data, successHandler) {
            this._setProperty(data, successHandler, "_loginDocumentStorageKey");
        };

        this.getLoginDocument = function () {
            return this._getProperty("_loginDocumentStorageKey");
        };

        this.setSessionDocument = function (data, successHandler) {
            this._setProperty(data, successHandler, "_sessionDocumentStorageKey");
        };

        this.getSessionDocument = function () {
            return this._getProperty("_sessionDocumentStorageKey");
        };

        // Helper method to set a property
        // by adding it to local storage and then calling a success handler.
        this._setProperty = function (data, successHandler, propStorageKey) {
            if (this[propStorageKey]) {
                if (data !== null) {
                    var storage = getStorage();
                    if (storage === null) { storage = {}; }
                    storage[this[propStorageKey]] = data;
                    setStorage(storage);
                } else {
                    this._clearProperty(propStorageKey);
                }
                if (successHandler && typeof(successHandler) === "function") {
                    successHandler();
                }
            }
        };

        // Helper method to get a property
        // by grabbing it from local storage.
        this._getProperty = function (propStorageKey) {
            var storage = getStorage();
            if (storage !== null) {
                return storage[this[propStorageKey]];
            }
            return null;
        };

        this._clearProperty = function (propStorageKey) {
            var storage = getStorage();
            if (storage !== null) {
                delete storage[this[propStorageKey]];
            }
            setStorage(storage);
        };

        this.clear = function() { clearStorage(); };

});