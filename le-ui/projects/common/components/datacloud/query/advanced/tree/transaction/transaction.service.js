angular.module('common.datacloud.query.builder.tree.transaction.service', [])
    .service('QueryTreeTransactionService', function () {
        var QueryTreeTransactionService = this;

        this.getAmtConfig = function () {
            return {
                from: { name: 'from-amt', value: undefined, position: 0, type: 'Amt' },
                to: { name: 'to-amt', value: undefined, position: 1, type: 'Amt' }
            };
        }

        this.getQtyConfig = function () {
            return {
                from: { name: 'from-qty', value: undefined, position: 0, type: 'Qty' },
                to: { name: 'to-qty', value: undefined, position: 1, type: 'Qty' }
            };
        }

        this.getTimeConfig = function () {

        }
        this.getCmpsList = function () {
            return [
                { 'name': 'EVER', 'displayName': 'Ever' },
                { 'name': 'IN_CURRENT', 'displayName': 'Current' },
                { 'name': 'PREVIOUS', 'displayName': 'Previous' },
                { 'name': 'PRIOR_OLY_LT', 'displayName': 'Only Prior to Last' },
                { 'name': 'BETWEEN_LT', 'displayName': 'Between Last' },
                { 'name': 'BETWEEN', 'displayName': 'Between' },
                { 'name': 'BEFORE', 'displayName': 'Before' },
                { 'name': 'AFTER', 'displayName': 'After' }
            ];
        }

        this.periodList = function () {
            return [
                { 'name': 'Week', 'displayName': 'Week' },
                { 'name': 'Month', 'displayName': 'Month' },
                { 'name': 'Quarter', 'displayName': 'Quarter' },
                { 'name': 'Year', 'displayName': 'Year' }
            ];
        }
        this.unitPurchasedCmpChoises = function () {
            return [
                { 'name': 'ANY', 'displayName': 'Any' },
                { 'name': 'GREATER_THAN', 'displayName': 'Greater Than' },
                { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater or Equal' },
                { 'name': 'LESS_THAN', 'displayName': 'Less Than' },
                { 'name': 'LESS_OR_EQUAL', 'displayName': 'Lesser or Equal' },
                { 'name': 'GTE_AND_LT', 'displayName': 'Between' }
            ];
        }

        this.amountSpentCmpChoises = function () {
            return [
                { 'name': 'ANY', 'displayName': 'Any' },
                { 'name': 'GREATER_THAN', 'displayName': 'Greater Than' },
                { 'name': 'GREATER_OR_EQUAL', 'displayName': 'Greater or Equal' },
                { 'name': 'LESS_THAN', 'displayName': 'Less Than' },
                { 'name': 'LESS_THAN', 'displayName': 'Lesser or Equal' },
                { 'name': 'GTE_AND_LT', 'displayName': 'Between' }
                
            ];
        }
    });