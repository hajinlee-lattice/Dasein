const UIActionsFactory = require('../uiactions-factory');
const jwt = require('jsonwebtoken');
const uuid = require('uuid/v4');

/**
 * Routing for '/reset' api
 * End points for UI defined here
 */
class ZendeskRouter {
    constructor(express, app, bodyParser, chalk, API_URL, PATH, request, proxies) {
        this.router = express.Router();
        this.chalk = chalk;
        this.API_URL = API_URL;
        this.PATH = PATH;
        this.request = request;
        this.proxies = proxies;
        this.log = console.log;
        app.use(bodyParser.json());
    }
    getPlsHost() {
        var plsUrl = this.proxies['/pls']['remote_host'];
        return plsUrl;
    }
    createRoutes() {

        /**
         * Intercepts all the requests for the path '/reset'
         */
        this.router.use(function timeLog(req, res, next) {
            var plsUrl = this.proxies['/pls']['remote_host'];
            var validationUrl = '/pls/tenantconfig';
            const options = {
                url: plsUrl + validationUrl,
                method: 'GET',
                headers: {
                    'Authorization': req.headers.authorization ? req.headers.authorization : ''
                }
            };

            this.request(options, function (error, response, body) {
                if (response.statusCode == 200) {
                    next();
                } else {
                    res.send(UIActionsFactory.getUIActionsObject('Unauthorized', 'Notice', 'Error'));
                }
            });
        }.bind(this));

        /**
         *  query params:
         *      userName: string
         */
        this.router.get('/token/', function (req, res) {
            var payload = {
                name: req.query.name,
                email: req.query.email,
                iat: (new Date().getTime() / 1000),
                external_id: req.query.email,
                exp: (new Date().getTime() / 1000) + 420,
                jti: uuid.v4()
            };
            var token = jwt.sign(payload, '52fb614cdedb5d2b1820db7bc01e9c64');
            res.send(token);
        }.bind(this));

        return this.router;
    }
}
module.exports = ZendeskRouter;