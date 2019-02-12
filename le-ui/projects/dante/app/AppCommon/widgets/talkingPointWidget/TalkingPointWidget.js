angular.module('mainApp.appCommon.widgets.TalkingPointWidget', [
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.appCommon.utilities.NumberUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])

.service('TalkingPointWidgetService', function (MetadataUtility, DateTimeFormatUtility, NumberUtility, ResourceUtility, LpiPreviewStore, URLUtility) {

    this.decodeHtmlEntities = function (text) {
        return $("<textarea/>").html(text).text();
    };

    this.evaluateLookupPath = function (token, account, playID, accountMetadata, leadMetadata, recId) {
        var regex = /^account\.(.*?)/i,
            value = "";

        if (regex.test(token)) {
            token = token.replace(regex, "$1");
            property = MetadataUtility.GetNotionProperty(token, accountMetadata);
            value = this.lookupHelper(account, token);
        } else {
            // if the token is a prelead property, look for the selected play, and find the TOKEN
            var play = this.getPlay(playID, account.SalesforceAccountID, recId);
            property = MetadataUtility.GetNotionProperty(token, leadMetadata);
            value = this.lookupHelper(play, token);
        }

        if (property && !this.ignoreFormatType) {
            this.CurrentProperty = property;
            value = this.formatType(value, property.PropertyTypeString);
        }

        return value;
    };

    /* 
        Date attributes should be treated as formatted strings we receive from Ulysses - AR
    */
    this.formatType = function (value, type) {
        switch (type) {
            case MetadataUtility.PropertyType.CURRENCY:
                value = ResourceUtility.getString("CURRENCY_SYMBOL") + NumberUtility.AbbreviateLargeNumber(value, 1);
                break;
            case MetadataUtility.PropertyType.EPOCH_TIME:
                value = new Date(value * 1000).toLocaleDateString();
                break;
            // case MetadataUtility.PropertyType.DATE_TIME:
            //     value = DateTimeFormatUtility.FormatJsonDateCSharpFormat(value, "DATETIME");
            //     break;
            default:
                break;
        }

        return value;
    };

    //finds a property on a javascript object ignoring case
    //http://stackoverflow.com/questions/12484386/access-javascript-property-case-insensitively
    this.lookupHelper = function (object, property) {
        if (object === null || property === null) {
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
    this.getPlay = function (playID, sfdcAccountId, recId) {
        if (URLUtility.LpiPreview() === 'true') {
            return LpiPreviewStore.getLead();
        }

        if (playID === null || sfdcAccountId === null) {
            return null;
        }

        var localStorageKey = 'DanteLead';
        var leadObj = $.jStorage.get(localStorageKey);
        if (leadObj === null) {
            localStorageKey += '-' + sfdcAccountId;
            leadObj = $.jStorage.get(localStorageKey);
        }

        if (leadObj === null) {
            return null;
        }

        for (var i = 0; i < leadObj.listData.length; i++) {
            var leadData = leadObj.listData[i];

            if (leadData.Data && leadData.Data.PlayID) {
                if (typeof recId !== 'undefined') {
                    if (leadData.Data.RecommendationID.toString() === recId.toString()) {
                        return leadData.Data;
                    }
                } else if (leadData.Data.PlayID === playID) {
                    return leadData.Data;
                }
            } else {
                if (leadData.PlayID === playID) {
                    return leadData;
                }
            }
        }

        return null;
    };

    // XXX Replace with something that will asynchronously write an error to the log database.
    this.logError = function (error) {
        //console.log(error);
    };

    this.FormatTalkingPoint = function (content, account, playID, metadata, recId) {
        var self = this;
        var tokens = /\{!([^\}]*)\}/g;
        var conditions = /\[([^\]]*)\]([^\[]*)\[end\]/gi;

        var accountMetadata = MetadataUtility.GetNotionMetadata("DanteAccount", metadata);
        var leadMetadata = MetadataUtility.GetNotionMetadata("DanteLead", metadata);

        // XXX If data is stored globally, these don't need to be closures.
        function replaceToken(match, token) {
            var result;
            // Need to handle special tokens that are not evaluated attributes (ENG-7180)
            switch (token) {
                case "Space":
                    result = "&nbsp;";
                    break;
                case "Today":
                    var today = new Date();
                    result = DateTimeFormatUtility.Today();
                    break;
                case "Now":
                    var now = new Date();
                    result = DateTimeFormatUtility.Today(true);
                    break;
                case "NowUTC":
                    var nowUTC = new Date();
                    result = DateTimeFormatUtility.TodayUTC(true);
                    break;
                case "Newline":
                    result = "<br/>";
                    break;
                default:
                    result = self.evaluateLookupPath(token, account, playID, accountMetadata, leadMetadata, recId);
                    break;

            }

            switch (result) {
                case undefined:
                    self.logError("Could not evaluate lookup path " + token);
                    break;
                case null:
                    result = "";
                    break;
            }

            return result;
        }

        function replaceCondition(match, condition, text) {
            function replaceAndQuoteToken(match, token) {
                self.ignoreFormatType = true;
                var value = "'" + replaceToken(match, token) + "'";
                delete self.ignoreFormatType;
                return value;
            }

            var x = condition;

            x = condition.replace(tokens, replaceAndQuoteToken);
            x = self.decodeHtmlEntities(x);
            x = x.replace(/([^<>])=/g, "$1==");
            x = x.replace(/<>/g, "!=");
            x = x.replace(/([^a-zA-Z0-9.])?(True)([^a-zA-Z0-9.])?/gi, "$1true$3");
            x = x.replace(/([^a-zA-Z0-9.])?(False)([^a-zA-Z0-9.])?/gi, "$1false$3");
            x = x.replace(/[^a-zA-Z0-9.]OR[^a-zA-Z0-9.]/g, "||");
            x = x.replace(/[^a-zA-Z0-9.]AND[^a-zA-Z0-9.]/g, "&&");
            x = x.replace(/[^a-zA-Z0-9.]null[^a-zA-Z0-9.]/g, "''");

            /*
                FIXME:  The middle regex breaks on multi-word strings, so the bookend regex's
                        will replace spaces in strings with the unicode snowman character.
                        This is not a very elegant solution at all, but works for now...
            */
            x = x.replace(/"[^\\"\n]*(\\["\\][^\\"\n]*)*"|'[^\\'\n]*(\\['\\][^\\'\n]*)*'/g, function(match) { return match.replace(' ','☃'); });
            x = x.replace(/(\s+|^|\||&)((?:\S+?)\s*?(?:==|!=|<=|>=|>|<)\s*?(?:\S+?))(\s+|$|\||&)/g, "$1($2)$3");
            x = x.replace(/"[^\\"\n]*(\\["\\][^\\"\n]*)*"|'[^\\'\n]*(\\['\\][^\\'\n]*)*'/g, function(match) { return match.replace('☃',' '); });

            try {
                var evaluation = talkingPointParser.parse(x);

                if (evaluation) {
                    return text;
                }
            } catch (e) {
                self.logError(e);
            }

            return "";
        }

        content = content.replace(conditions, replaceCondition);
        content = content.replace(tokens, replaceToken);

        return content;
    };

})

.controller('TalkingPointWidgetController', function ($scope, $element, TalkingPointWidgetService, ResourceUtility, URLUtility) {
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var parentData = $scope.parentData;

    if (data === null) {
        return;
    }

    var recId = URLUtility.CrmRecommendation();

    var talkingPointText = TalkingPointWidgetService.FormatTalkingPoint(data.Content, parentData, data.PlayID, metadata, recId);
    // greedy match for a string with only whitespace and bracketed expressions
    // ie: talkingPointText = '<p></p>';
    var regex = /(<[^>]*>|\s)*/;
    var matches = talkingPointText.match(regex);

    if (matches[0].length == talkingPointText.length) {
        talkingPointText = ResourceUtility.getString('DANTE_NO_TALKING_POINTS_FOR_SECTION');
    }
    $scope.talkingPointText = talkingPointText;

    // ensures this font size cleanup only occurs once.
    if (window.TalkingPointsCleanupTimer) {
        clearTimeout(window.TalkingPointsCleanupTimer);
        delete window.TalkingPointsCleanupTimer;
    }

    window.TalkingPointsCleanupTimer = setTimeout(function() {
        var map = {
            "1":1, "2":2, "3":3, "4":4, "5":5, "6":6, "7":7,
            "8":1, "10":2, "12":3, "14":4, "18":5, "24":6, "36":7
        };

        $('font[size]').each(function(k, font) {
            font.size = (map[parseInt(font.size)] || 2);
        });

        delete window.TalkingPointsCleanupTimer;
    }, 1);

    // Need to handle link clicks differently in Salesforce1 (ENG-8201)
    setTimeout(function() {
        $('a', $element).click(function (evt) {
            if(typeof sforce != 'undefined' && sforce != null) {
                evt.stopImmediatePropagation();
                evt.preventDefault();
                sforce.one.navigateToURL(this.href, true);
            }
        });
    }, 1);
})

.directive('talkingPointWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/talkingPointWidget/TalkingPointWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});