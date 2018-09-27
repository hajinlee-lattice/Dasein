angular.module('common.banner', [])
.service('Banner', function($sce) {
    var Banner = this;

    this.init = function() {
        this.banners = [];
    };

    this.get = function(name) {
        if (name) {
            return this.banners.filter(function(banner) {
                return banner.name === name;
            });
        } else {
            return this.banners;
        }
    };

    this.set = function(opts) {
        opts = opts || {};

        var banner = {
            badge: 1,
            name: opts.name || "",
            show: opts.show || true,
            type: opts.type || '',
            title: opts.title || '',
            message: opts.message.replace("home.jobs","home.jobs.data") || ''
        };

        var old = opts.name ? this.get(opts.name) : [];
        
        old = old.filter(function(item) {
            return (item.type == banner.type && item.title == banner.title && item.message.toString() == banner.message.toString());
        });

        if (old.length > 0) {
            old.forEach(function(item) {
                item.badge++;
            });
        } else {
            this.banners.push(banner);
        }
    };

    this.reset = function() {
        this.banners.length = 0;
    };

    this.generate = function(type, opts) {
        this.set(angular.extend({
            type: type || 'info'
        }, opts));
    };

    this.error = function(opts) {
        this.generate('error', opts);
    };

    this.warning = function(opts) {
        this.generate('warning', opts);
    };

    this.success = function(opts) {
        this.generate('success', opts);
    };

    this.info = function(opts) {
        this.generate('info', opts);
    };
 
    this.init();
})
.directive('ngHtmlCompile', function($compile) {
    return {
        restrict: 'A',
        link: function(scope, element, attrs) {
            scope.$watch(attrs.ngHtmlCompile, function(newValue, oldValue) {
                element.html(newValue);
                $compile(element.contents())(scope);
            });
        }
    };
})
.component('bannerMessage', {
    templateUrl: '/components/banner/banner.component.html',
    controller: function(Banner) {
        var vm = this;

        vm.$onInit = function() {
            vm.banners = Banner.get();
        };

        vm.isVisible = function() {
            var visible = false;

            vm.banners.forEach(function(banner, index) {
                if (banner.show === true) {
                    visible = true;
                }
            });

            return visible;
        };
    }
});