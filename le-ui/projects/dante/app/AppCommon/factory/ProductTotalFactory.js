angular.module('mainApp.appCommon.factory.ProductTotalFactory', [
    'mainApp.appCommon.utilities.ConfigConstantUtility'
])
.factory('ProductTotal', function (ConfigConstantUtility) {
    var ProductTotal = function (RepresentativeAccounts) {
        this.RepresentativeAccounts = RepresentativeAccounts || 1;

        this.TotalSpend = 0;
        this.TotalVolume = 0;
        this.TransactionCount = 0;
        this.AverageSpend = 0;
    };

    ProductTotal.prototype.aggregate = function (toBeCombined) {
        // FIXME: handle if properties of toBeCombined does not contain the following
        // and if RepresentativeAccounts are not the same
        this.TotalVolume += toBeCombined.TotalVolume;
        this.TotalSpend += toBeCombined.TotalSpend;
        this.TransactionCount += toBeCombined.TransactionCount;
        this.AverageSpend = this.TotalSpend / this.RepresentativeAccounts;
    };

    return ProductTotal;
});
