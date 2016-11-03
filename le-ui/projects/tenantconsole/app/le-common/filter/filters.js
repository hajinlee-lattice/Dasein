angular.module('le.common.filter.filters', [])
.filter('joinList', function () {
    return function (input, sep) {
        if (angular.isArray(input)) {
            return input.join(sep);
        } else {
            return input;
        }
    };
})
.filter('startFrom', function () {
    return function (input, start) {
        if (input) {
            start = +start;
            return input.slice(start);
        }
        return [];
    };
});
