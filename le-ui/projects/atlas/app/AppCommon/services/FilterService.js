    angular.module('mainApp.appCommon.services.FilterService', [
])
.service('FilterService', function () {
    this.init = function() {
        this.filters = null;
    }

    this.clear = function() {
        this.init();
    }

    this.setFilters = function(key, obj, debug) {
        if(key && obj) {
            this.filters = this.filters || {};
            this.filters[key] = obj;

            if(debug) {
                console.log('FilterService.setFilters', key, obj);
            }
        }

    }

    this.getFilters = function(key, debug) {
        if(debug) {
            console.log('FilterService.getFilters', key, (this.filters && this.filters[key] ? this.filters[key] : null));
        }
        if(key) {
            if(!this.filters || !this.filters[key]) {
                return null;
            }
            return this.filters[key];
        }
        return this.filters;
    }
});
