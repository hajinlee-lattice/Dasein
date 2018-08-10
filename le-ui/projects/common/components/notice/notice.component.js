angular.module('common.notice', [])
.service('Notice', function($timeout) {
    var Notice = this;

    this.init = function() {
        this.notice = {
            visible: false,
            title: '',
            message: '',
            delay: 0
        };
    };

    this.get = function() {
        return this.notice;
    };

    this.set = function(opts) {
        opts = opts || {};

        this.notice.visible = true;
        this.notice.type = opts.type || 'info';
        this.notice.title = opts.title || '';
        this.notice.message = opts.message || '';

        $timeout(function() {
            Notice.notice.visible = false;
        }, opts.delay || 3500);
    };

    this.reset = function() {
        this.init();
    };

    this.generate = function(type, opts) {
        delete opts.type;

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
.component('noticeMessage', {
    templateUrl: '/components/notice/notice.component.html',
    controller: function(Notice) {
        this.notice = Notice.get();
    }
});