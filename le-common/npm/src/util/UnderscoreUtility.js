(function(){
    var app = angular.module('le.common.util.UnderscoreUtility', [
    ]);

    app.factory('_', function() {
        return window._; // assumes underscore has already been loaded on the page
    });
}).call();
