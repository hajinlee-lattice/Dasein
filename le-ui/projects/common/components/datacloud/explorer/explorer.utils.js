export default {
    FallbackSrc: function () {
        'ngInject';

        var fallbackSrc = {
            link: function postLink(scope, iElement, iAttrs) {
                iElement.bind('error', function () {
                    angular.element(this).attr("src", iAttrs.fallbackSrc);
                    angular.element(this).css({ display: 'initial' }); // removes onerror hiding image
                });
            }
        }

        return fallbackSrc;
    },
    /**
     * Module to manipulate the rule object
     */
    ExplorerUtils: function () {
        'ngInject';

        var ExplorerUtils = this;
        ExplorerUtils.CONTACT_ENTITY = 'Contact';
        ExplorerUtils.ACCOUNT_ENTITY = 'Account';

        /**
         * Create an empty bucketRestriction with ignored=true
         * @param {*} entity 
         * @param {*} attributeName 
         */
        function getEmptyBucketRestriction(entity, attributeName) {
            var bucketRestriction = {
                bucketRestriction: {
                    attr: entity + '.' + attributeName,
                    bkt: {},
                    ignored: true
                }
            };
            return bucketRestriction;
        }

        /**
         * 
         * @param {*} singleBucket object containing account_restriction and contact_restriction 
         * @param {*} entity 
         * @param {*} attr 
         */
        function addRestrictionToBucket(singleBucket, entity, attr) {

            switch (entity) {
                case ExplorerUtils.CONTACT_ENTITY: {
                    singleBucket.contact_restriction.logicalRestriction.restrictions.push(getEmptyBucketRestriction(entity, attr));
                    break;
                }
                default: {
                    singleBucket.account_restriction.logicalRestriction.restrictions.push(getEmptyBucketRestriction(entity, attr));
                }
            }
        }

        /**
         * 
         * @param {*} restrictions restrictions contained in the in either account_restriction or contact_restriction
         * @param {*} entityAttr string containing the Entity.AttributeName combined
         */
        function removeFromRestrictions(restrictions, entityAttr) {
            for (var i = 0; i < restrictions.length; i++) {
                if (restrictions[i].bucketRestriction.attr === entityAttr) {
                    restrictions.splice(i, 1);
                }
            }
        }

        /**
             * Provied the Entity it takes care of calling the remove method on the appropriate bucket
             * @param {*} bucket 
             * @param {*} entity 
             * @param {*} attr 
             */
        function removeRestrictionFromBucket(bucket, entity, attr) {
            switch (entity) {
                case ExplorerUtils.CONTACT_ENTITY: {
                    removeFromRestrictions(bucket.contact_restriction.logicalRestriction.restrictions, entity + '.' + attr);
                    break;
                }
                default: {
                    removeFromRestrictions(bucket.account_restriction.logicalRestriction.restrictions, entity + '.' + attr);
                }
            }
        }

        /**
         * For each bucket (A,B, .....) it adds/removes the attribute
         * @param {*} add
         * @param {*} rule Object containing ratingRule.bucketToRuleMap
         * @param {*} entity 
         * @param {*} attr 
         */
        this.removeAddAttrFromRule = function (add, rule, entity, attr) {
            var buckets = rule.ratingRule.bucketToRuleMap;
            var bucketsName = Object.keys(buckets);
            bucketsName.forEach(function (bucket) {
                var bkt = buckets[bucket];
                switch (add) {
                    case true: {
                        addRestrictionToBucket(bkt, entity, attr);
                        break;
                    }
                    case false: {
                        removeRestrictionFromBucket(bkt, entity, attr);
                        break;
                    }
                    default: {
                        console.warn('Add/Remove to the bucket not performed (', bucket, attr, ')');
                    }
                }
            });
        };

    }
}; 