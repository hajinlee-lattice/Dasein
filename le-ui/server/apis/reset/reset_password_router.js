const UIActionsFactory = require('../uiactions-factory');
const jwt = require('jsonwebtoken');
const uuid = require('uuid/v4');

/**
 * Routing for '/reset' api
 * End points for UI defined here
 */
class ResetPasswordRouter {
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
            this.log('Time: ', Date.now(), req.headers, this.proxies);
            next();
        }.bind(this));

        /**
         *  query params:
         *      userName: string
         */
        this.router.get('/publish/', function (req, res) {
            if(!req.query.jwt || req.query.jwt == null || req.query.jwt.trim() == ''){
                res.status(400).send(UIActionsFactory.getUIActionsObject('Reset Password', 'Notice', 'Request missing userName'));
                return;
            }
            try{
                var payload = jwt.verify(req.query.jwt, 'e6b6376591584a6990e3a56306247cc2');

                let body = {
                    Username: payload.username,
                    HostPort: this.getPlsHost(),
                    Product: 'Lead Prioritization'
                };
                let options = {
                    url: this.getPlsHost() + '/pls/forgotpassword',
                    method: 'PUT',
                    timeout: 3000,
                    headers: {
                        "accept": "application/json, text/plain, */*",
                        "accept-language": "en-US,en;q=0.9",
                        "cache-control": "no-cache",
                        "content-type": "application/json",
                        "pragma": "no-cache"
                    },
                    mode: "cors",
                    body: JSON.stringify(body)
                };
                this.request(options, (error, response, body) => {
                    this.log('Call back ', error, response, body);
                    if(body) {
                        res.status(200).send(UIActionsFactory.getUIActionsObject('Reset Password', 'Banner', payload.username + ' password Reset'));
                    }else{
                        res.status(response.statusCode).send(UIActionsFactory.getUIActionsObject('Reset Password', 'Banner', 'Error Password Reset'));
                    }
                });
            }catch(err) {
                res.status(500).send(UIActionsFactory.getUIActionsObject('Reset Password', 'Banner', 'Generic server error'));
            }

        }.bind(this));

         /**
         *  query params:
         *      userName: string
         */
        this.router.get('/confirm/', function (req, res) {
            if(!req.query.userName || req.query.userName == null || req.query.userName.trim() == ''){
                res.status(400).send(UIActionsFactory.getUIActionsObject('Reset Password', 'Notice', 'Request missing userName'));
                return;
            }
            //will be called by reset password button
            //will send /reset/publish?userName={JWT} to pramods api to be included in email
            var payload = {
                username: req.query.userName,
                jti: uuid.v4()
            };
            var token = jwt.sign(payload, 'e6b6376591584a6990e3a56306247cc2', { expiresIn: '1d' });
            res.status(200).send(token);
        }.bind(this));

        return this.router;
    }
}
module.exports = ResetPasswordRouter;