angular.module('mainApp.appCommon.services.PurchaseHistoryService', [
    'mainApp.appCommon.utilities.PurchaseHistoryUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.factory.ProductTotalFactory',
    'mainApp.appCommon.factory.ProductTreeFactory'
])
.service('PurchaseHistoryService', function (ProductTree, ProductTotal, MetadataUtility, PurchaseHistoryUtility) {
    var PeriodSet = ['M', 'Q'];

    /*
    * productHierarchies - converts a flat hierarchy list into a product tree
    */
    this.createProductTree = function (productHierarchies) {
        var self = this;
        var productTree = new ProductTree('root', []);

        var levelName, levelNames, currentHierarchy, currentLevel;
        for (var hierarchyId in productHierarchies) {
            currentLevel = productTree;
            levelNames = self.getProductFromHierarchyId(productHierarchies, hierarchyId);

            for (var i = 0; i < levelNames.length; i++) {
                levelName = levelNames[i];
                if (levelName !== null) {
                    if (!currentLevel.nodes[levelName]) {
                        currentLevel = currentLevel.addNode(new ProductTree(levelName, levelNames.slice(0, i+1)));
                    } else {
                        currentLevel = currentLevel.nodes[levelName];
                    }
                } else {
                    break;
                }
            }
        }

        return productTree;
    };

    /*
    * converts array into object with id as key
    */
    this.mapProductHierarchyToId = function (danteProductHierarchy) {
        return danteProductHierarchy.reduce(function(memo, item) {
            memo[item.ProductHierarchyID] = item;
            return memo;
        }, {});
    };

    /*
    * returns levelNames given a product hierarchy id
    */
    this.getProductFromHierarchyId = function (danteProductHierarchy, productHierarchyID) {
        var product = danteProductHierarchy[productHierarchyID];
        if (!product) {
            return [];
        }

        var levelNames = [], levelName;
        for (var k = 1; k < 4; k++) {
            levelName = product['Level'+k+'Name'];
            if (levelName !== null) {
                levelNames.push(levelName);
            } else {
                break;
            }
        }

        return levelNames;
    };

    /*
    * returns product hierarchy id given the levelNames
    */
    this.getProductHierarchyIdFromProduct = function (danteProductHierarchy, productLevelNames) {
        var productLevelLimit = 3; // current limit is 3 hierarchy level
        var counter = 0; // match counter, 3 matches = found

        var currentLevel, currentLevelName;
        for (var k in danteProductHierarchy) {
            counter = 0;
            currentLevel = danteProductHierarchy[k];
            for (var i = 0; i < productLevelLimit; i++) {
                currentLevelName = currentLevel['Level'+(i+1)+'Name'];
                if (productLevelNames[i] === currentLevelName) {
                    counter++;
                } else if (productLevelNames[i] === undefined && currentLevelName === null) {
                    // match undefined (from array) with null (from db)
                    counter++;
                }
            }
            if (counter === productLevelLimit) {
                return k;
            }
        }

        return null;
    };

    /*
    * takes a proudct tree and pushes purchase history data into tree
    */
    this.constructPurchaseHistory = function (productTree, purchaseHistory, danteProductHierarchy, contextId, representativeAccounts) {
        var self = this;
        var attributes = purchaseHistory.PurchaseHistoryAttributes;
        var period = purchaseHistory.Period;
        var periodStartDate = purchaseHistory.PeriodStartDate;

        var periodIndex = PeriodSet.indexOf(period);
        // not found eg 'D' for daily, treat as smallest, which is month
        periodIndex = (periodIndex === -1) ? 0 : periodIndex;


        var attribute, currentLevel, levelName, levelNames, date, nextPeriod, nextPeriodId;
        for (var i = 0; i < attributes.length; i++) {
            attribute = attributes[i];
            levelNames = self.getProductFromHierarchyId(danteProductHierarchy, attribute.ProductHierarchyID);
            currentLevel = productTree.getNode(levelNames);
            if (!currentLevel) {
                // this should never happen: it means there is a purchase history attribute without a correct product hierarchy
                continue;
            }

            date = PurchaseHistoryUtility.convertPeriodOffsetToDate(period, periodStartDate, attribute.PeriodOffset);

            for (var j = periodIndex; j < PeriodSet.length; j++) {
                nextPeriod = PeriodSet[j];
                nextPeriodId = date.format(PurchaseHistoryUtility.periodToMomentFormat[nextPeriod]);
                if (!currentLevel.data[nextPeriod]) {
                    currentLevel.data[nextPeriod] = {};
                }
                if (!currentLevel.data[nextPeriod][nextPeriodId]) {
                    currentLevel.data[nextPeriod][nextPeriodId] = {};
                }
                if (!currentLevel.data[nextPeriod][nextPeriodId][contextId]) {
                    currentLevel.data[nextPeriod][nextPeriodId][contextId] = new ProductTotal(representativeAccounts);
                }

                currentLevel.data[nextPeriod][nextPeriodId][contextId].aggregate(attribute);
            }
        }

        var momentFormat = PurchaseHistoryUtility.periodToMomentFormat.M;
        var dateStart = new Date(periodStartDate * 1000);

        productTree.periodStartDate = (productTree.periodStartDate && dateStart > productTree.periodStartDate) ? productTree.periodStartDate : dateStart;

        // aggregate on non leaf node
        self.depthFirstAggregate(productTree, contextId);

        return productTree;
    };

    /*
    * post order
    * recursively traverse product tree and aggregates children values onto node
    */
    this.depthFirstAggregate = function (productTree, contextId) {
        var self = this;
        var nodes = productTree.nodes;
        var data = productTree.data;

        var node, period;
        for (var n in nodes) {
            node = nodes[n];
            self.depthFirstAggregate(node, contextId);
            for (var p = 0; p < PeriodSet.length; p++) {
                period = PeriodSet[p];
                if (!data[period]) {
                    data[period] = {};
                }
                for (var periodId in node.data[period]) {
                    if (!data[period][periodId]) {
                        data[period][periodId] = {};
                    }
                    // if child node has data, then aggregate onto current node
                    if (node.data[period][periodId][contextId]) {
                        if (!data[period][periodId][contextId]) {
                            data[period][periodId][contextId] = new ProductTotal(node.data[period][periodId][contextId].RepresentativeAccounts);
                        }

                        data[period][periodId][contextId].aggregate(node.data[period][periodId][contextId]);
                    }
                }
            }
        }
    };

    this.getYoyStat = function (diff) {
        var yoyStat = null;

        if (diff > 0) {
            yoyStat = 'increase';
        } else if (diff < 0) {
            yoyStat = 'decrease';
        }

        return yoyStat;
    };

    this.getSpendDiff = function (spend, spendToCompare) {
        var diff = null;

        if (spend !== null && spendToCompare !== null) {
            diff = spend - spendToCompare;
        } else if (spend !== null) {
            diff = spend;
        } else if (spendToCompare !== null) {
            diff = -spendToCompare;
        }

        return diff;
    };

    /*
    * get data for a specified period id
    */
    this.getPeriodDataFromProduct = function (productNode, period, periodId, account, segment) {
        var self = this;

        var accountTotalSpend = null,
            accountPrevYearSpend = null,
            accountQuarterTotalSpend = null,
            accountPrevQuarterTotalSpend = null,
            segmentAverageSpend = null;

        var productData = productNode.data;
        if (!productData[period]) {
            return {
                periodId: periodId,
                accountTotalSpend: accountTotalSpend,
                accountPrevYearSpend: accountPrevYearSpend,
                accountQuarterTotalSpend: accountQuarterTotalSpend,
                accountPrevQuarterTotalSpend: accountPrevQuarterTotalSpend,
                segmentAverageSpend: segmentAverageSpend
            };
        }

        var periodData = productData[period][periodId];
        if (periodData) {
            var accountData = periodData[account];
            if (accountData) {
                accountTotalSpend = accountData.TotalSpend;
            }
            var segmentData = periodData[segment];
            if (segmentData) {
                segmentAverageSpend = segmentData.AverageSpend;
            }
        }

        var prevYearPeriodId = PurchaseHistoryUtility.getPrevYearPeriod(periodId);
        var prevYearData = productData[period][prevYearPeriodId];
        if (prevYearData) {
            var segmentPrevYearData = prevYearData[segment];
            if (segmentPrevYearData) {
            }
            var accountPrevYearData = prevYearData[account];
            if (accountPrevYearData) {
                accountPrevYearSpend = accountPrevYearData.TotalSpend;
            }
        }

        var quarterPeriodId = PurchaseHistoryUtility.getQuarterFromPeriodId(period, periodId);
        if (quarterPeriodId) {
            var quarterData = productData.Q[quarterPeriodId];
            if (quarterData) {
                var accountQuarterData = quarterData[account];
                if (accountQuarterData) {
                    accountQuarterTotalSpend = accountQuarterData.TotalSpend;
                }
            }

            var prevQuarterPeriodId = PurchaseHistoryUtility.getPrevQuarterPeriod('Q', quarterPeriodId);
            var prevQuarterData = productData.Q[prevQuarterPeriodId];
            if (prevQuarterData) {
                var accountPrevQuarterData = prevQuarterData[account];
                if (accountPrevQuarterData) {
                    accountPrevQuarterTotalSpend = accountPrevQuarterData.TotalSpend;
                }
            }
        }

        var yoyDiff = self.getSpendDiff(accountTotalSpend, accountPrevYearSpend);
        var yoyStat = self.getYoyStat(yoyDiff);

        return {
            periodId: periodId,
            yoyStat: yoyStat,
            yoyDiff: yoyDiff,
            accountTotalSpend: accountTotalSpend,
            accountPrevYearSpend: accountPrevYearSpend,
            accountQuarterTotalSpend: accountQuarterTotalSpend,
            accountPrevQuarterTotalSpend: accountPrevQuarterTotalSpend,
            segmentAverageSpend: segmentAverageSpend
        };
    };

    this.constructProduct = function (productTree, account, segment, period, periodRange) {
        var self = this;
        var accountProductTotalSpend = null,
            accountProductTotalPrevYearSpend = null,
            segmentProductTotalAverageSpend = null;

        var productPeriods = periodRange.map(function (periodId) {
            var periodData = self.getPeriodDataFromProduct(productTree, period, periodId, account, segment);

            var accountTotalSpend = periodData.accountTotalSpend;
            var accountPrevYearSpend = periodData.accountPrevYearSpend;
            var segmentAverageSpend = periodData.segmentAverageSpend;
            var accountQuarterTotalSpend = periodData.accountQuarterTotalSpend;
            var accountPrevQuarterTotalSpend = periodData.accountPrevQuarterTotalSpend;

            if (accountTotalSpend !== null) {
                accountProductTotalSpend = +accountProductTotalSpend + accountTotalSpend;
            }

            if (accountPrevYearSpend !== null) {
                accountProductTotalPrevYearSpend = +accountProductTotalPrevYearSpend + accountPrevYearSpend;
            }

            if (segmentAverageSpend !== null) {
                segmentProductTotalAverageSpend = +segmentProductTotalAverageSpend + segmentAverageSpend;
            }

            return {
                periodId: periodId,
                yoyStat: periodData.yoyStat,
                accountTotalSpend: accountTotalSpend,
                accountPrevYearSpend: accountPrevYearSpend,
                segmentAverageSpend: segmentAverageSpend,
                accountQuarterTotalSpend: accountQuarterTotalSpend,
                accountPrevQuarterTotalSpend: accountPrevQuarterTotalSpend
            };
        });

        var yoyDiff = self.getSpendDiff(accountProductTotalSpend, accountProductTotalPrevYearSpend);
        var yoyStat = self.getYoyStat(yoyDiff);
        var segmentDiff = self.getSpendDiff(accountProductTotalSpend, segmentProductTotalAverageSpend);

        return {
            displayName: productTree.displayName,
            periods: productPeriods,
            yoyStat: yoyStat,
            yoyDiff: yoyDiff,
            segmentDiff: segmentDiff,
            accountProductTotalSpend: accountProductTotalSpend,
            accountProductTotalPrevYearSpend: accountProductTotalPrevYearSpend,
            segmentProductTotalAverageSpend: segmentProductTotalAverageSpend,
            hasChildren: productTree.hasChildren()
        };
    };

    this.constructProducts = function (productTree, account, segment, period, periodRange) {
        var self = this;
        var nodes = productTree.nodes;

        var products = Object.keys(nodes).map(function (product) {
            return self.constructProduct(nodes[product], account, segment, period, periodRange);
        });

        return products;
    };

    // filter segment properties that danteAccount has a property for
    this.filterAccountSegmentProperties = function (segmentPropertiesNameMap, danteAccount) {
        var accountSegments = {}; // <segmentCategory, segmentName>
        for (var prop in danteAccount) {
            if (segmentPropertiesNameMap[prop]) {
                accountSegments[prop] = danteAccount[prop];
            }
        }
        return accountSegments;
    };

    // accountSegments - segments on the account
    // segmentAccount - account with IsSegment = true
    this.getAccountSegments = function (metadata, danteAccount, segmentAccounts) {
        var self = this;

        var notionMetadata = MetadataUtility.GetNotionMetadata('DanteAccount', metadata);
        if (notionMetadata == null || notionMetadata.Properties == null) {
            return null;
        }

        // all properties that have PropertyTypeString = 'Segment'
        var notionSegmentProperties = MetadataUtility.GetNotionPropertiesOfKeyValue("PropertyTypeString", MetadataUtility.PropertyType.SEGMENT, notionMetadata);
        if (notionSegmentProperties == null) {
            return null;
        }

        // map to Name property
        var segmentPropertiesNameMap = {}; // <name, propertyObj>
        notionSegmentProperties.forEach(function (property) {
            segmentPropertiesNameMap[property.Name] = property;
        });

        var accountSegments = self.filterAccountSegmentProperties(segmentPropertiesNameMap, danteAccount);

        var segments = [];
        // skip if account belongs to no segments
        if (Object.keys(accountSegments).length > 0) {
            segmentAccounts.forEach(function (segmentAccount) {
                // get segment properties on the segmentAccount
                var segmentAccountSegmentProperties = self.filterAccountSegmentProperties(segmentPropertiesNameMap, segmentAccount);

                // matched flag initalized to false if segmentAccountSegmentProperties is empty
                var matched = Object.keys(segmentAccountSegmentProperties).length > 0;
                // match segmentCategory from segmentAccount with danteAccount
                for (var segmentCategory in segmentAccountSegmentProperties) {
                    if (segmentAccountSegmentProperties[segmentCategory] !== accountSegments[segmentCategory]) {
                        matched = false;
                        break;
                    }
                }

                // matched = true means all segment properties on the segmentAccount is also on danteAccount
                if (matched) {
                    segments.push({
                        segmentName: segmentAccount.DisplayName,
                        segmentAccountId: segmentAccount.BaseExternalID,
                        representativeAccounts: segmentAccount.RepresentativeAccounts
                    });
                }
            });
        }

        return segments;
    };
});
