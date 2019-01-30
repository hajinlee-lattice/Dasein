angular.module('mainApp.appCommon.widgets.PurchaseHistoryWidget.stores.PurchaseHistoryStore', [
    'mainApp.core.services.NotionService',
    'mainApp.appCommon.utilities.PurchaseHistoryUtility',
    'mainApp.appCommon.services.PurchaseHistoryService',
    'mainApp.appCommon.factory.ProductTreeFactory'
])
.service('PurchaseHistoryStore', function ($rootScope, $q, ProductTree, NotionService, PurchaseHistoryUtility, PurchaseHistoryService) {

    this.purchaseHistoryList = [];
    this.periodRange = [];
    this.segment = null;
    this.accountId = null;
    this.period = 'M';
    this.danteProductHierarchy = [];
    this.productTree = null;
    this.productTreeSelectedPeriods = null;

    this.initPurchaseHistory = function (accountId, metadata) {
        var self = this;
        if (self.productTree) {
            var deferred = $q.defer();
            deferred.resolve(null);
            return deferred.promise;
        }

        self.accountId = accountId;
        var danteAccount, segmentAccounts, productHierarchy, accountPurchaseHistory;
        return $q.all([
            NotionService.findOne('DanteAccount', accountId),
            NotionService.findItemsByKey('DanteAccount', true, 'IsSegment'),
            NotionService.findAll('DanteProductHierarchy'),
            NotionService.findOneByKey('DantePurchaseHistory', accountId, 'account'),
        ])
        .then(function(results) {
            var errors = {};
            if (results[0] === null || results[0].resultObj === null) {
                errors.Account = results[0];
            }
            if (results[2] === null || results[2].length === 0) {
                errors["Product Hierarchy"] = results[2];
            }
            if (results[3] === null || results[3].resultObj === null) {
                errors["Purchase History"] = results[3];
            }
            if (Object.keys(errors).length) {
                var deferred = $q.defer();
                deferred.reject(errors);
                return deferred.promise;
            }

            danteAccount = results[0].resultObj;
            segmentAccounts = results[1];
            productHierarchy = results[2];
            accountPurchaseHistory = results[3].resultObj;
            self.danteProductHierarchy = productHierarchy;
            self.danteProductHierarchy = PurchaseHistoryService.mapProductHierarchyToId(self.danteProductHierarchy);
            self.firstTransactionDate = accountPurchaseHistory.FirstTransactionDate ? accountPurchaseHistory.FirstTransactionDate * 1000 : -1;
            self.finalTransactionDate = accountPurchaseHistory.FinalTransactionDate ? accountPurchaseHistory.FinalTransactionDate * 1000 : -1;

            self.productTree = PurchaseHistoryService.createProductTree(self.danteProductHierarchy);

            // make a copy of product tree, used for building tree for selected periods range
            self.productTreeSelectedPeriods = angular.copy(self.productTree);
            // set up descendants
            self.productTreeSelectedPeriods.depthFirstTraversal(function (node) {

                // skip root becaue it won't matter
                if (node.displayName !== 'root' && node.levelsId.length > 0) {
                    node.descendants[node.displayName] = node.displayName;
                    var nodes = node.nodes;
                    for (var child in nodes) {
                        angular.extend(node.descendants, nodes[child].descendants);
                    }
                }
            });

            // populate the tree with purchase history data
            self.productTree = PurchaseHistoryService.constructPurchaseHistory(self.productTree, accountPurchaseHistory, self.danteProductHierarchy, self.accountId, danteAccount.RepresentativeAccounts);
            // keep track which set of purchase history is being added to product tree
            self.purchaseHistoryList.push(accountId);

            var segments = PurchaseHistoryService.getAccountSegments(metadata, danteAccount, segmentAccounts);

            console.log('initPurchaseHistory', accountId, self.accountId, results[1], segments)

            self.segment = segments[0];
            if (self.segment) {
                return NotionService.findOneByKey('DantePurchaseHistory', self.segment.segmentAccountId, 'spendanalyticssegment');
            } else {
                self.segment = {};
                return {};
            }
        })
        .then(function (result) {
            if (result.resultObj) {
                var segmentPurchaseHistory = result.resultObj;
                self.productTree = PurchaseHistoryService.constructPurchaseHistory(self.productTree, segmentPurchaseHistory, self.danteProductHierarchy, self.segment.segmentAccountId, self.segment.representativeAccounts);
                self.purchaseHistoryList.push(self.segment.segmentAccountId);
            }

            var momentFormat = PurchaseHistoryUtility.periodToMomentFormat[self.period];

            var momentFinalDate;
            if (self.finalTransactionDate > -1 && self.finalTransactionDate < new Date().getTime()) {
                momentFinalDate = moment(self.finalTransactionDate);
            } else {
                momentFinalDate = moment();
            }

            var momentFirstDate;
            if (self.firstTransactionDate > -1 && self.firstTransactionDate < new Date().getTime()) {
                momentFirstDate = moment(self.firstTransactionDate);
            } else { 
                // Fall back to periodStartDate if firstTransactionDate is not available
                momentFirstDate = moment(self.productTree.periodStartDate);
            }

            self.endPeriodId = momentFinalDate.format(momentFormat);
            self.startPeriodId = momentFinalDate.add(-11, 'months').format(momentFormat);

            self.periodEndDatePeriodId = self.endPeriodId;
            self.periodStartDatePeriodId = momentFirstDate.format(momentFormat);
            if (PurchaseHistoryUtility.periodIdComparator(self.startPeriodId, self.periodStartDatePeriodId) < 0) {
                self.startPeriodId = self.periodStartDatePeriodId;
            }

            console.log(self.periodEndDatePeriodId);
            console.log(self.periodStartDatePeriodId);

            self.setPeriodRange();
            self.setSelectedPeriodsTree();
        });
    };

    this.setSelectedPeriodsTree = function () {
        var self = this;

        var maxSpendYoy = null,
            maxSpendSegmentDiff = null;

        self.productTreeSelectedPeriods.depthFirstTraversal(function (node) {
            var productNode = self.productTree.getNode(node.levelsId);
            node.expanded = false;
            node.data = PurchaseHistoryService.constructProduct(productNode, self.accountId, self.segment.segmentAccountId, self.period, self.periodRange);

            // skip root because root aggregate should not be included in max
            if (node.displayName !== 'root' && node.levelsId.length > 0) {
                maxSpendYoy = Math.max(maxSpendYoy,
                    node.data.accountProductTotalSpend,
                    node.data.accountProductTotalPrevYearSpend
                );
                maxSpendSegmentDiff = Math.max(maxSpendSegmentDiff,
                    Math.abs(PurchaseHistoryService.getSpendDiff(node.data.accountProductTotalSpend,
                    node.data.segmentProductTotalAverageSpend))
                );
            }

            if (angular.isObject(node.nodes)) {
                var nodes = [];
                for (var key in node.nodes) {
                    nodes.push(node.nodes[key]);
                }
                node.nodes = nodes;
            }
        });

        self.productTreeSelectedPeriods.data.maxSpendYoy = maxSpendYoy;
        self.productTreeSelectedPeriods.data.maxSpendSegmentDiff = maxSpendSegmentDiff;
    };

    this.setPeriodRange = function () {
        this.periodRange = PurchaseHistoryUtility.getPeriodRange(this.period, this.startPeriodId, this.endPeriodId);
    };

    this.setPeriod = function (period) {
        var oldMomentFormat = PurchaseHistoryUtility.periodToMomentFormat[this.period];
        var newMomentFormat = PurchaseHistoryUtility.periodToMomentFormat[period];
        this.period = period;

        // update startPeriodId, endPeriodId to appropriate
        this.startPeriodId = moment(this.startPeriodId, oldMomentFormat).format(newMomentFormat);
        this.endPeriodId = moment(this.endPeriodId, oldMomentFormat).format(newMomentFormat);

        this.setPeriodRange();
        this.setSelectedPeriodsTree();

        $rootScope.$broadcast('purchaseHistoryStoreChange');
    };

    this.setPeriodIdRange = function (startPeriodId, endPeriodId) {
        this.startPeriodId = startPeriodId;
        this.endPeriodId = endPeriodId;
        this.setPeriodRange();
        this.setSelectedPeriodsTree();

        $rootScope.$broadcast('purchaseHistoryStoreChange');
    };
});
