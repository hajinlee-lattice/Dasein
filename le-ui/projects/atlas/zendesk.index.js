import httpService from "../common/app/http/http-service";
import Observer from "../common/app/http/observer";

var disable_zendesk = false;

if (['app.lattice-engines.com'].indexOf(window.location.host) < 0 && ['testapp.lattice-engines.com'].indexOf(window.location.host) < 0) { // production or qa
    disable_zendesk = true;
}

if (!disable_zendesk) {
    window.zE || (function (e, t, s) { var n = window.zE = window.zEmbed = function () { n._.push(arguments) }, a = n.s = e.createElement(t), r = e.getElementsByTagName(t)[0]; n.set = function (e) { n.set._.push(e) }, n._ = [], n.set._ = [], a.async = true, a.setAttribute("charset", "utf-8"), a.src = "https://static.zdassets.com/ekr/asset_composer.js?key=" + s, n.t = +new Date, a.type = "text/javascript", r.parentNode.insertBefore(a, r) })(document, "script", "7b73cc63-fb76-4deb-9df8-09130f1d9931");

    let loginDocument = $.jStorage.get('GriotLoginDocument');
    if(loginDocument){
        let userName = loginDocument.FirstName + ' ' + loginDocument.LastName;
        let userEmail = loginDocument.UserName;
        let tokenDocument = $.jStorage.get('GriotTokenDocument');

        let observer = new Observer(response => {

            window.zESettings = {
                authenticate: { jwt: response.data }
            };

            window.zE(function () {
                zE.identify({ name: userName, email: userEmail });
            });
        });

        httpService.get(`/zdsk/token?name=${userName}&email=${userEmail}`, observer, {Authorization: tokenDocument});
    }
}