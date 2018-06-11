'use strict';

describe('ProductTreeFactorySpec Tests', function () {
    var productTree, tree, periodMap = ['M', 'Q'];

    beforeEach(function () {
        module('mainApp.appCommon.factory.ProductTreeFactory');
        inject(['ProductTree',
            function (ProductTree) {
                productTree = ProductTree;
                tree = new productTree('root');
            }
        ]);
    });

    describe('ProductTree', function () {
        it('should be a constructor (function)', function () {
            expect(typeof productTree).toBe('function');
        });

        it('should be the constructor', function () {
            expect(tree instanceof productTree).toBeTruthy();
        })
    });

    describe('ProductTree properties', function () {
        it('should contain property displayName', function () {
            expect(tree.displayName).toBeDefined();
            expect(tree.displayName).toEqual('root');
        });

        it('should contain property data', function () {
            expect(tree.data).toBeDefined();
            expect(typeof tree.data).toBe('object');
        });

        it('should contain property nodes', function () {
            expect(tree.nodes).toEqual({});
        });
    });

    describe('ProductTree.addNode', function () {
        var tree1;

        it('should add a ProductTree node and return the node', function () {
            tree1 = tree.addNode(new productTree('tree1'));
            expect(tree.nodes['tree1']).toBeDefined();
            expect(tree1 instanceof productTree).toBeTruthy();
        }); 
    });

    describe('ProductTree.hasChildren', function () {
        var tree1;

        it('should be empty', function () { 
            expect(tree.hasChildren()).toBeFalsy();
        });

        it('should not be empty', function () {
            tree1 = tree.addNode(new productTree('tree1'));
            expect(tree.hasChildren()).toBeTruthy();
        });
    });

    describe('ProductTree.hasData', function () {
        it('should not have data', function () {
            expect(tree.hasData()).toBeFalsy();
        });

        it('should have data', function () {
            tree.data.M = {};
            tree.data.M['foo'] = {foo:'bar'};
            expect(tree.hasData()).toBeTruthy();
        });
    });
    describe('ProductTree.getNode', function () {
        var tree1, node;

        beforeEach(function () { 
            tree1 = tree.addNode(new productTree('tree1'));
        });

        it('should retrieve node', function () {
            node = tree.getNode(['tree1'])
            expect(node).toBeDefined();
            expect(node.displayName).toEqual('tree1');
        });

        it('should not retrieve a node not in tree', function () {
            node = tree.getNode(['tree2'])
            expect(node).toBeNull();
        });
    });

    describe('ProductTree.depthFirstTraversal', function () {
        beforeEach(function () {
        /*          Root
                    / | \
                   A  B  C
                     / \  
                     D  E
                         \F
        */
            var A = tree.addNode(new productTree('A'));
            var B = tree.addNode(new productTree('B'));
            var C = tree.addNode(new productTree('C'));
            var D = B.addNode(new productTree('D'));
            var E = B.addNode(new productTree('E'));
            var F = E.addNode(new productTree('F'));
        });

        it('should postorder depth first traversal', function () {
            var order = ['A', 'D', 'F', 'E', 'B', 'C', 'root'];
            var result = [];
            tree.depthFirstTraversal(function (node) {
                result.push(node.displayName);
            });

            expect(angular.equals(result, order)).toBeTruthy();
        })
    })
});
