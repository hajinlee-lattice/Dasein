angular.module('mainApp.appCommon.utilities.URLUtility', [
])
.service('URLUtility', function () {
    /**
    * Returns the location of the web server based on the main page
    */
    this.GetWebServerAddress = function (mainPage) {
        if (mainPage == null) {
            return null;
        }
        var pathname = location.pathname;
        var pageIndex = pathname.indexOf(mainPage);

        if (pageIndex != -1) {
            pathname = pathname.substring(0, pageIndex);
        }
        else {
            pathname = pathname.substring(0, pathname.length - 1);
        }

        return location.protocol + "//" + location.host + pathname;
    };

    this.GetBaseUrl = function () {
        return location.protocol + "//" + location.host + location.pathname;
    };

    this.GetUrlQueryStrings = function (url) {
        if (url == null) {
            return "";
        }
        var result = "";

        //First remove the hash if it is found
        var hashIndex = url.indexOf("#");
        if (hashIndex != -1) {
            url = url.substring(0, hashIndex);
        }

        //Find the beginning of the query string and remove everything before it
        var qIndex = url.indexOf("?");
        if (qIndex != -1) {
            result = url.substring(qIndex, url.length);
        }
        return result;
    };

    this.GetUrlWithQueryStrings = function (baseUrl) {
        if (baseUrl == null) {
            return null;
        }
        var url = this.HandleProtocol(baseUrl);
        var queryStringParameters = this.GetUrlQueryStrings(window.location.href);
        if (queryStringParameters != null && queryStringParameters != "?") {
            url += queryStringParameters;
        }
        return url;
    };

    this.RemoveQueryStringParameter = function (url, parameter) {
        var urlHash = "";
        var hashIndex = url.indexOf("#");
        var qIndex = url.indexOf('?');

        if (hashIndex > -1) {
            if (hashIndex < qIndex) {
                // hash is before the query string parameters
                urlHash = url.substring(hashIndex, qIndex);
            } else {
                // hash is at the end
                urlHash = url.substring(hashIndex);
                url = url.replace(urlHash, "");
            }
        }

        var urlparts = url.split('?');

        if (urlparts.length >= 2) {
            var prefix = encodeURIComponent(parameter) + '=';
            var pars = urlparts[1].split(/[&;]/g);
            for (var i = pars.length; i-- > 0; )               // reverse iteration as may be destructive
                if (pars[i].lastIndexOf(prefix, 0) !== -1)   // idiom for string.startsWith
                    pars.splice(i, 1);
            if (pars.length === 0) {
                url = urlparts[0];
            } else {
                url = urlparts[0] + '?' + pars.join('&');
            }
        }

        if (hashIndex > -1 && hashIndex < qIndex) {
            return url;
        } else {
            return url + urlHash;
        }
    };

    this.OpenWindow = function (url, location) {
        if (url == null || location == null) {
            return;
        }
        window.open(url, location);
    };

    this.OpenNewWindow = function (url) {
        this.OpenWindow(url, '_blank');
    };

    this.OpenPostWindow = function (target, url, data, fetchType) {
        var content = this.ConstructPostContent(url, data, fetchType);
        var win = window.open("about:blank", target);
        if (win) {
            win.document.write(content);
        }

        return win;
    };

    this.OpenPostIframe = function (target, url, data, fetchType) {
        var content = this.ConstructPostContent(url, data, fetchType);
        $("#" + target).html(content);
    };

    this.ConstructPostContent = function (url, data, fetchType) {
        fetchType = typeof fetchType !== 'undefined' ? fetchType : 'POST';
        var content = "<!DOCTYPE html><html><head></head><body>";
        content += "<form id=\"postForm\" method='" + fetchType + "' action='" + url + "'>";
        $.each(data, function (name, value) {
            content += "<input type='hidden' name='" + name + "' value='" + value + "'></input>";
        });
        content += "</form>";
        content += "<script type='text/javascript'>document.getElementById(\"postForm\").submit();</script>";
        content += "</body></html>";

        return content;
    };

    this.GetQueryStringValue = function (key, keepHash) {
        keepHash = typeof keepHash === 'boolean' ? keepHash : false;
        var result = null;
        var payload = window.location.href;
        var qIndex = payload.indexOf("?");
        if (qIndex != -1) {
            var queryString = payload.substring(qIndex + 1);

            var pair = null;
            var params = queryString.split("&");
            for (i = 0; i < params.length; i++) {

                pair = params[i].split("=");

                if (pair.length == 2 && pair[0] == key) {
                    result = pair[1];
                    break;
                }
            }
        }

        //cleanup: if the result contains a hash "#"
        //remove the hash and anything after
        if (result != null && !keepHash) {
            var hashIndex = result.indexOf("#");
            if (hashIndex != -1) {
                result = result.substring(0, hashIndex);
            }
        }

        return (result != null ? this.Decode(result) : null);
    };

    this.Decode = function (value) {
        var result = value;

        var matches = result.match(new RegExp("%..", "gi"));
        if (matches != null) {
            for (i = 0; i < matches.length; i++) {
                var charCode = parseInt("0x" + matches[i].substring(1));
                var character = String.fromCharCode(charCode);
                result = result.replace(matches[i], character);
            }
        }

        return result;
    };

    this.HandleProtocol = function (url) {
        if (url == null) {
            return null;
        }
        var httpIncluded = url.indexOf("http://") === 0;
        var httpsIncluded = url.indexOf("https://") === 0;
        if (httpIncluded || httpsIncluded) {
            return url;
        } else {
            return location.protocol + "//" + url;
        }

    };

});