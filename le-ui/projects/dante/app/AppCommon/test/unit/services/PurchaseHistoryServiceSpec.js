'use strict';

describe('PurchaseHistoryServiceSpec Tests', function () {
    var purchaseHistoryService,
        productTree,
        productHierarchies,
        mappedProductHierarchies,
        sampleDailyPurchaseHistory,
        sampleMonthlyPurchaseHistory,
        sampleProductTreeFromDaily,
        sampleProductTreeFromMonthly,
        sampleProductTreeFromDailyAtLevel3,
        moment,
        accountId = 'SAMPLE_ACCOUNT',
        segmentId = 'SAMPLE_SEGMENT';

    beforeEach(function () {
        module('mainApp.appCommon.services.PurchaseHistoryService');
        module('mainApp.appCommon.factory.ProductTreeFactory');
        module('test.testData.PurchaseHistoryTestDataService');
        inject(['PurchaseHistoryService', 'ProductTree', 'PurchaseHistoryTestDataService', '$window',
            function (PurchaseHistoryService, ProductTree, PurchaseHistoryTestDataService, $window) {
                moment = $window.moment;
                purchaseHistoryService = PurchaseHistoryService;
                productTree = ProductTree;

                productHierarchies = PurchaseHistoryTestDataService.GetSampleProductHierarchies();
                mappedProductHierarchies = PurchaseHistoryTestDataService.GetSampleProductHierarchiesMapped();
                sampleDailyPurchaseHistory = PurchaseHistoryTestDataService.GetSampleDailyPurchaseHistory();
                sampleMonthlyPurchaseHistory = PurchaseHistoryTestDataService.GetSampleMonthlyPurchaseHistory();
                sampleProductTreeFromDaily = PurchaseHistoryTestDataService.GetSampleProductTreeFromDaily();
                sampleProductTreeFromMonthly = PurchaseHistoryTestDataService.GetSampleProductTreeFromMonthly();
                sampleProductTreeFromDailyAtLevel3 = PurchaseHistoryTestDataService.GetSampleProductTreeFromDailyAtLevel3();
            }
        ]);
    });

    describe('PurchaseHistoryService.createProductTree', function () {
        var tree;
        beforeEach(function () {
            tree = purchaseHistoryService.createProductTree(mappedProductHierarchies);
        });

        it('should return a productTree instance', function () {
            expect(tree instanceof productTree).toBeTruthy;
        });

        it('should have a node for each product hierarchy in the product tree', function () {
            var levels, node;
            productHierarchies.forEach(function (item) {
                levels = purchaseHistoryService.getProductFromHierarchyId(item.ProductHierarchyID);
                node = tree.getNode(levels);
                expect(node).toBeDefined();
            });
        });

        it('should have child nodes at root level', function () {
            expect(tree.hasChildren()).toBeTruthy();
        });

        it('should have child nodes at non root or non leaf levels', function () {
            expect(tree.getNode(['Finance', 'Services', 'Consulting']).hasChildren()).toBeFalsy();
        });

        it('should not have child nodes at leaf level', function () {
            expect(tree.getNode(['Finance', 'Services']).hasChildren()).toBeTruthy();
        });
    });

    describe('PurchaseHistoryService.mapProductHierarchyToId', function () {
        var toReturn;

        it('should map an array of product hierarchy to object where key is the ProductHierarchyID', function () {
            toReturn = purchaseHistoryService.mapProductHierarchyToId(productHierarchies);
            expect(Object.keys(toReturn).length).toEqual(productHierarchies.length);
        });
    });

    describe('PurchaseHistoryService.getProductFromHierarchyId', function () {
        var toReturn;
        it('should retreive product hierarchy names given the product hierarchy ID ', function () {
            toReturn = purchaseHistoryService.getProductFromHierarchyId(mappedProductHierarchies, 'PHch001');
            expect(angular.equals(toReturn, ['Finance', 'Services', 'Management'])).toBeTruthy();
        });

        it('should return empty array for invalid ID', function () {
            toReturn = purchaseHistoryService.getProductFromHierarchyId(mappedProductHierarchies, 'PHch999');
            expect(toReturn).toEqual([])
        });
    });

    describe('PurchaseHistoryService.getProductHierarchyIdFromProduct', function () {
        var toReturn;

        it('should retreive product hierarchy ID given the product hierarchy names', function () {
            toReturn = purchaseHistoryService.getProductHierarchyIdFromProduct(mappedProductHierarchies, ['Finance', 'Services', 'Consulting']);
            expect(toReturn).toEqual('PHch000');
        });

        it('should return null if products no found in product hierarchy', function () {
            toReturn = purchaseHistoryService.getProductHierarchyIdFromProduct(mappedProductHierarchies, ['foo', 'bar', 'baz']);
            expect(toReturn).toBeNull();
        });
    });

    describe('PurchaseHistoryService.depthFirstAggregate', function () {
        it('should aggregate high level data given a product tree with only data at level 3', function () {
            purchaseHistoryService.depthFirstAggregate(sampleProductTreeFromDailyAtLevel3, accountId, 1);

            expect(angular.equals(sampleProductTreeFromDailyAtLevel3,sampleProductTreeFromDaily)).toBeTruthy();
        });
    });

    // use angular deep equal to assert javascript objects
    // sampleProductTreeFromDaily/Monthly was created by hand, not with uut
    describe('PurchaseHistoryService.constructPurchaseHistory', function () {
        var hierarchyTree;
        var result, actual;

        beforeEach(function () {
            hierarchyTree = purchaseHistoryService.createProductTree(productHierarchies);
        });

        it('should add daily data to product tree', function () {
            hierarchyTree = purchaseHistoryService.constructPurchaseHistory(hierarchyTree, sampleDailyPurchaseHistory, mappedProductHierarchies, accountId);

            // FIXME: iterate hierarchy tree, look for node in sample data, assert
            result = hierarchyTree.getNode(['Finance', 'Services', 'Management']).data;
            actual = sampleProductTreeFromDaily.nodes['Finance'].nodes['Services'].nodes['Management'].data;
            expect(angular.equals(result, actual)).toBeTruthy();

            result = hierarchyTree.getNode(['Finance', 'Services', 'Consulting']).data;
            actual = sampleProductTreeFromDaily.nodes['Finance'].nodes['Services'].nodes['Consulting'].data;
            expect(angular.equals(result, actual)).toBeTruthy();

            result = hierarchyTree.getNode(['Finance', 'Services']).data;
            actual = sampleProductTreeFromDaily.nodes['Finance'].nodes['Services'].data;
            expect(angular.equals(result, actual)).toBeTruthy();

            result = hierarchyTree.getNode(['Finance']).data;
            actual = sampleProductTreeFromDaily.nodes['Finance'].data;
            expect(angular.equals(result, actual)).toBeTruthy();
        });

        it('should add monthly data to product tree', function () {
            hierarchyTree = purchaseHistoryService.constructPurchaseHistory(hierarchyTree, sampleMonthlyPurchaseHistory, mappedProductHierarchies, accountId);

            result = hierarchyTree.getNode(['Finance', 'Services', 'Management']).data;
            actual = sampleProductTreeFromMonthly.nodes['Finance'].nodes['Services'].nodes['Management'].data;
            expect(angular.equals(result, actual)).toBeTruthy();

            result = hierarchyTree.getNode(['Finance', 'Services', 'Consulting']).data;
            actual = sampleProductTreeFromMonthly.nodes['Finance'].nodes['Services'].nodes['Consulting'].data;
            expect(angular.equals(result, actual)).toBeTruthy();

            result = hierarchyTree.getNode(['Finance', 'Services']).data;
            actual = sampleProductTreeFromMonthly.nodes['Finance'].nodes['Services'].data;
            expect(angular.equals(result, actual)).toBeTruthy();

            result = hierarchyTree.getNode(['Finance']).data;
            actual = sampleProductTreeFromMonthly.nodes['Finance'].data;
            expect(angular.equals(result, actual)).toBeTruthy();
        });

        it('should add multiple purchase history (segments) into same product tree', function () {
            // add the same purchase history, but as two different accounts
            hierarchyTree = purchaseHistoryService.constructPurchaseHistory(hierarchyTree, sampleMonthlyPurchaseHistory, mappedProductHierarchies, accountId);
            hierarchyTree = purchaseHistoryService.constructPurchaseHistory(hierarchyTree, sampleMonthlyPurchaseHistory, mappedProductHierarchies, segmentId);

            // picking random data point
            var node = hierarchyTree.getNode(["Finance", "Services", "Consulting"]);
            var periodData = node.data.M;
            var period =  Object.keys(periodData)[0]; 
            expect(periodData[period][accountId]).toBeDefined();
            expect(periodData[period][segmentId]).toBeDefined();
        });
    });

    describe('PurchaseHistoryService.getSpendDiff', function () {
        var toReturn;
        
        it('should return negative difference if less than', function () {
            toReturn = purchaseHistoryService.getSpendDiff(1,2);
            expect(toReturn).toEqual(-1);
        });

        it('should return positive difference if greater than', function () {
            toReturn = purchaseHistoryService.getSpendDiff(2,1);
            expect(toReturn).toEqual(1);
        });

        it('should return 0 if equal', function () {
            toReturn = purchaseHistoryService.getSpendDiff(1,1);
            expect(toReturn).toEqual(0);
        });
    });

    describe('PurchaseHistoryService.getYoyStat', function () {
        var toReturn;
        
        it('should return "decrease" if less than', function () {
            toReturn = purchaseHistoryService.getYoyStat(-1);
            expect(toReturn).toEqual('decrease');
        });

        it('should return "increase" if greater than', function () {
            toReturn = purchaseHistoryService.getYoyStat(1);
            expect(toReturn).toEqual('increase');
        });

        it('should return null if 0', function () {
            toReturn = purchaseHistoryService.getYoyStat(0);
            expect(toReturn).toEqual(null);
        });
    });

    xdescribe('PurchaseHistoryService.getPeriodDataFromProduct', function () {});
    xdescribe('PurchaseHistoryService.constructProduct', function () {});
    xdescribe('PurchaseHistoryService.constructProducts', function () {});
    xdescribe('PurchaseHistoryService.filterAccountSegmentProperties', function () {});
    xdescribe('PurchaseHistoryService.getAccountSegments', function () {});
});
