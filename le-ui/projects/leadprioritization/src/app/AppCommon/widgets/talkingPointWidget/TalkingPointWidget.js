angular.module('mainApp.appCommon.widgets.TalkingPointWidget', [
])

.service('TalkingPointWidgetService', function () {
    
    
    this.decodeHtmlEntities = function (text) {
        text = text.replace(/&gt;/g, ">");
        text = text.replace(/&lt;/g, "<");
        text = text.replace(/&#39;/g, "'");
        text = text.replace(/&apos;/g, "'");

        return text;
    };

    this.evaluateLookupPath = function (token, account, playID, accountMetadata, leadMetadata) {
        var toReturn = "";

        // if the token is an account property, look for the account.TOKEN on the account
        var regex = /^account\.(.*?)/i;
        if (regex.test(token)) {
            token = token.replace(regex, "$1");
            property = MetadataUtility.GetNotionProperty(token, accountMetadata);
            toReturn = this.lookupHelper(account, token);
        } else {
            // if the token is a prelead property, look for the selected play, and find the TOKEN
            var play = this.getPlay(playID, account.SalesforceAccountID);
            property = MetadataUtility.GetNotionProperty(token, leadMetadata);
            toReturn = this.lookupHelper(play, token);
        }

        if (property) {
            switch (property.PropertyTypeString) {
                case MetadataConstants.PropertyType.CURRENCY:
                    toReturn = CurrencyUtil.FormatCurrency(toReturn);
                    break;
                case MetadataConstants.PropertyType.EPOCH_TIME:
                    toReturn = new Date(toReturn * 1000).toLocaleDateString();
                    break;
                default:
                    break;
            }
        }

        return toReturn;
    };

    //finds a property on a javascript object ignoring case
    //http://stackoverflow.com/questions/12484386/access-javascript-property-case-insensitively
    this.lookupHelper = function (object, property) {
        if (object == null || property == null) {
            return undefined;
        }
        property = property.toLowerCase();
        for (var p in object) {
            if (object.hasOwnProperty(p) && property == p.toLowerCase()) {
                return object[p];
            }
        }
        return undefined;
    };
    
    // This is a hack. We shouldn't be referencing hardcoded properties
    this.getPlay = function (playID, sfdcAccountId) {
        if (playID == null || sfdcAccountId == null) {
            return null;
        }
        
        var localStorageKey = "DanteLead-" + sfdcAccountId;
        var leadObj = $.jStorage.get(localStorageKey);
        if (leadObj == null) {
            return null;
        }
        
        for (var i = 0; i < leadObj.listData.length; i++) {
            if (leadObj.listData[i].PlayID === playID) {
                return leadObj.listData[i];
            }
        }
        
        return null;
    };

    // XXX Replace with something that will asynchronously write an error to the log database.
    this.logError = function (error) {

    };
    
    this.FormatTalkingPoint = function (content, account, playID) {
        var self = this;
        var tokens = /\{!([^\}]*)\}/g;
        var conditions = /\[([^\]]*)\]([^\[]*)\[end\]/g;

        var accountMetadata = MetadataUtility.GetNotionMetadata("DanteAccount", options.metadata);
        var leadMetadata = MetadataUtility.GetNotionMetadata("DanteLead", options.metadata);

        // XXX If data is stored globally, these don't need to be closures.
        function replaceToken(match, token, offset, string, quote) {
            result = self.evaluateLookupPath(token, account, playID, accountMetadata, leadMetadata);
            if (result === undefined) {
                self.logError("Could not evaluate lookup path " + token);
            }

            return result;
        }

        function replaceCondition(match, condition, text, offset, string) {
            function replaceAndQuoteToken(match, token, offset, string) {
                return "'" + replaceToken(match, token, offset, string) + "'";
            }

            var x = condition;

            x = condition.replace(tokens, replaceAndQuoteToken);
            x = self.decodeHtmlEntities(x);
            x = x.replace(/([^<>])=/g, "$1==");
            x = x.replace(/<>/g, "!=");
            x = x.replace(/[^a-zA-Z0-9.]True[^a-zA-Z0-9.]/g, "true");
            x = x.replace(/[^a-zA-Z0-9.]False[^a-zA-Z0-9.]/g, "false");
            x = x.replace(/[^a-zA-Z0-9.]OR[^a-zA-Z0-9.]/g, "||");
            x = x.replace(/[^a-zA-Z0-9.]AND[^a-zA-Z0-9.]/g, "&&");
            x = x.replace(/[^a-zA-Z0-9.]null[^a-zA-Z0-9.]/g, "''");
            x = x.replace(/(\s+|^|\||&)((?:\S+?)\s*?(?:==|!=|<=|>=|>|<)\s*?(?:\S+?))(\s+|$|\||&)/g, "$1($2)$3");

            try {
                var evaluation = talkingPointParser.parse(x);
                if (evaluation) {
                    return text;
                }
            }
            catch (e) {
                self.logError(e);
            }

            return "";
        }

        content = content.replace(conditions, replaceCondition);
        content = content.replace(tokens, replaceToken);

        return content;
    };
    
})

.controller('TalkingPointWidgetController', function ($scope, $element, TalkingPointWidgetService) {
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var parentData = $scope.parentData;
    
    if(data == null) {
        return;
    }

    var talkingPointText = TalkingPointWidgetService.FormatTalkingPoint(data.Content, parentData, data.PlayID);

    // greedy match for a string with only whitespace and bracketed expressions
    // ie: talkingPointText = '<p></p>';
    var regex = /(<[^>]*>|\s)*/;
    var matches = talkingPointText.match(regex);

    if (matches[0].length == talkingPointText.length) {
        talkingPointText = ResourceUtil.getString('DANTE_NO_TALKING_POINTS_FOR_SECTION');
    }

    $scope.talkingPointText = talkingPointText;
})

.directive('talkingPointWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/talkingPointWidget/TalkingPointWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});