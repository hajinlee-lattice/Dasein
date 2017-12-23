angular.module('common.datacloud.query.builder.tree')
    .service('QueryTreeService', function () {
        console.log('TEST service');
        this.cmpMap = {
            "Yes": "is",
            "No": "is",
            "": "is",
            "is": "is",
            "empty": "is empty",
            "between": "is between",
            'IS_NULL': 'is empty',
            'IS_NOT_NULL': 'is present',
            'EQUAL': 'is equal to',
            'NOT_EQUAL': 'is not equal to',
            'GREATER_THAN': 'is greater than',
            'GREATER_OR_EQUAL': 'is greater than or equal to',
            'LESS_THAN': 'is less than',
            'LESS_OR_EQUAL': 'is less than or equal to',
            'GTE_AND_LTE': 'is greater or equal and lesser or equal',
            'GTE_AND_LT': 'is greater or equal and less than',
            'GT_AND_LTE': "is greater than and lesser or equal",
            'GT_AND_LT': "is greater than and less than",
            'IN_COLLECTION': 'in collection',
            'CONTAINS': 'contains',
            'NOT_CONTAINS': 'not contains',
            'STARTS_WITH': 'starts with'
        };
        this.numerical_operations = {
            'IS_NULL': 'Empty',
            'IS_NOT_NULL': 'Not Empty',
            'EQUAL': 'Equal',
            'NOT_EQUAL': 'Not Equal',
            'GREATER_THAN': 'Greater Than',
            'GREATER_OR_EQUAL': 'Greater or Equal',
            'LESS_THAN': 'Less Than',
            'LESS_OR_EQUAL': 'Lesser or Equal',
            'GTE_AND_LTE': '>= and <=',
            'GTE_AND_LT': '>= and <',
            'GT_AND_LTE': "> and <=",
            'GT_AND_LT': "> and <"
        },
            this.enum_operations = {
                'EQUAL': 'Is Equal To',
                'NOT_EQUAL': 'Does Not Equal'
            };
        this.no_inputs = [
            'IS_NULL',
            'IS_NOT_NULL'
        ];
        this.two_inputs = [
            'GTE_AND_LTE',
            'GTE_AND_LT',
            'GT_AND_LTE',
            'GT_AND_LT'
        ];
    });