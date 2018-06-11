angular.module('mainApp.appCommon.services.PurchaseHistoryTooltip', [

])
.service('PurchaseHistoryTooltip', function ($filter) {
    var PurchaseHistoryTooltip = this;

    this.show = function (el, $event, data) {
        var left = $event.target.offsetLeft - el.outerWidth()/2 + $event.target.clientWidth/2,
            top = $event.target.offsetTop - el.outerHeight() - 20;
        el.html(PurchaseHistoryTooltip.template(data));
        el.css({left: left, top: top});
        el.addClass('active');
    };

    this.hide = function (el) {
        el.removeClass('active');
    };

    this.template = function (data) {
        var tmpl = '';
        tmpl += '<li><span>Spend</span><span class="float-right">'+$filter('formatDollar')(data.accountTotalSpend)+'</span></li>';
        tmpl += '<li><span>Last Year Spend</span><span class="float-right">'+$filter('formatDollar')(data.accountPrevYearSpend)+'</span></li>';
        tmpl += '<li><hr></li>';
        tmpl += '<li><span>Quarterly Spend</span><span class="float-right">'+$filter('formatDollar')(data.accountQuarterTotalSpend)+'</span></li>';
        tmpl += '<li><span>Last Quarter</span><span class="float-right">'+$filter('formatDollar')(data.accountPrevQuarterTotalSpend)+'</span></li>';
        return tmpl;
    };

});
