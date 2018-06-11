angular.module('mainApp.appCommon.factory.ProductTreeFactory', [
])
.factory('ProductTree', function () {
    var ProductTree = function (displayName, levelsId) {
        this.displayName = displayName;
        this.levelsId = levelsId;

        this.nodes = {};
        this.data = {};

        // for search
        this.descendants = {}; // Set key=key
        // for display
        this.expanded = false;
    };

    ProductTree.prototype.hasChildren = function () {
        return Object.keys(this.nodes).length > 0;
    };

    ProductTree.prototype.hasData = function () {
        return this.data ? Object.keys(this.data).length > 0 : false;
    };

    ProductTree.prototype.addNode = function (node) {
        this.nodes[node.displayName] = node;
        return this.nodes[node.displayName];
    };

    /*
    * levels - ordered array with hierarchy names 
    *   e.g ['Product10', 'Product21', 'Product33']
    */ 
    ProductTree.prototype.getNode = function (levels) {
        if (!Array.isArray(levels) || levels.length === 0) {
            return this;
        }
        var current = this;

        for (var i = 0; i < levels.length && current; i++) {
            current = current.nodes[levels[i]] ? current.nodes[levels[i]] : null;
        }

        return current;
    };

    // post order, so things can bubble up to parents
    ProductTree.prototype.depthFirstTraversal = function (callback, continueCondition) {
        continueCondition = continueCondition || function () { return true; };

        var nodes = this.nodes;
        var node;
        if (continueCondition(node)) {
            if (angular.isArray(nodes)) {
                for (var i = 0; i < nodes.length; i++) {
                    node = nodes[i];
                    node.depthFirstTraversal(callback, continueCondition);
                }
            } else if (angular.isObject(nodes)) {
                for (var productName in nodes) {
                    node = nodes[productName];
                    node.depthFirstTraversal(callback, continueCondition);
                }
            }
            callback(this);
        }
    };

    ProductTree.prototype.expandAll = function () {
        this.depthFirstTraversal(function (node) {
          node.expanded = true;
        });
    };

    ProductTree.prototype.collapseAll = function () {
        this.depthFirstTraversal(function (node) {
          node.expanded = false;
        });
    };

    ProductTree.prototype.toggleExpand = function () {
        this.expanded = this.expanded ? false : true;
        if (!this.expanded) {
          this.collapseAll();
        }
    };

    // search descendants by displayName only
    ProductTree.prototype.searchTreeDescendants = function (toMatch) {
        if (!toMatch) {
            return true;
        }

        var isMatched = function (string, toMatch) {
            return string.toLowerCase().indexOf(toMatch.toLowerCase()) > -1;
        };

        // search self too
        if (isMatched(this.displayName, toMatch)) {
            return true;
        }

        for (var node in this.descendants) {
            if (isMatched(node, toMatch)) {
                return true;
            }
        }

        return false;
    };


    return ProductTree;
});
