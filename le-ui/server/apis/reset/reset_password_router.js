const UIActionsFactory = require('../uiactions-factory');
const jwt = require('jsonwebtoken');
const uuid = require('uuid/v4');
const url = require('url');

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
            //this.log('Time: ', Date.now(), req.headers, this.proxies);
            next();
        }.bind(this));

        /**
         *  query params:
         *      userName: string
         */
        this.router.get('/publish/', function (req, res) {
            if(!req.query.jwt || req.query.jwt == null || req.query.jwt.trim() == ''){
                res.status(400).send(UIActionsFactory.getUIActionsObject('Reset Password Failed', 'Notice', 'Request missing jwt'));
                return;
            }
            try{
                var payload = jwt.verify(req.query.jwt, 'e6b6376591584a6990e3a56306247cc2');

                //TODO: remove this hack for identifying which public URL to use
                let cleanHostPort = 'https://testapp.lattice-engines.com:443';
                if (/(?<!test)app\.lattice-engines\.com$/i.test(payload.hostport)) {
                    cleanHostPort = 'https://app.lattice-engines.com:443';
                } else if (/(?<!test)app2\.lattice-engines\.com$/i.test(payload.hostport)) {
                    cleanHostPort = 'https://app2.lattice-engines.com:443';
                }

                let body = {
                    Username: payload.username,
                    HostPort: cleanHostPort,
                    Product: 'Lead Prioritization'
                };
                let options = {
                    url: cleanHostPort + '/pls/forgotpassword',
                    method: 'PUT',
                    timeout: 3000,
                    headers: {
                        'accept': 'application/json, text/plain, */*',
                        'accept-language': 'en-US,en;q=0.9',
                        'cache-control': 'no-cache',
                        'content-type': 'application/json',
                        'pragma': 'no-cache'
                    },
                    mode: 'cors',
                    body: JSON.stringify(body)
                };
                this.request(options, (error, response, body) => {
                    this.log('Call back ', error, response, body);
                    if(body) {
                        res.redirect(303, '/login/form');
                    }else{
                        res.status(500).send(UIActionsFactory.getUIActionsObject('Reset Password Failed', 'Banner', 'There was an error reseting your password.'));
                    }
                });
            }catch(err) {
                res.status(500).send(UIActionsFactory.getUIActionsObject('Reset Password Failed', 'Banner', 'There was an error reseting your password.'));
            }

        }.bind(this));

        //will be called by reset password button
        //will send /reset/publish?userName={JWT} to pramods api to be included in email
        /**
         *  query params:
         *      userName: string
         */
        this.router.get('/confirm/', function (req, res) {
            if(!req.query.userName || req.query.userName == null || req.query.userName.trim() == ''){
                res.status(400).send(UIActionsFactory.getUIActionsObject('Reset Password Failed', 'Banner', 'There was an error reseting your password.'));
                return;
            }

            var payload = {
                username: req.query.userName,
                hostport: req.query.hostPort,
                jti: uuid.v4() //should be stored and used to prevent replay attacks
            };

            var token = jwt.sign(payload, 'e6b6376591584a6990e3a56306247cc2', { expiresIn: '1d' });

            let body = {
                userEmail: payload.username,
                hostPort: payload.hostport + '/reset/publish?jwt=' + token
            };
            let options = {
                url: this.getPlsHost() + '/pls/forgotpasswordconfirmation',
                method: 'PUT',
                timeout: 3000,
                headers: {
                    'accept': 'application/json, text/plain, */*',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'no-cache',
                    'content-type': 'application/json',
                    'pragma': 'no-cache'
                },
                mode: 'cors',
                body: JSON.stringify(body)
            };

            this.request(options, (error, response, body) => {
                this.log('Call back ', error, response, body);
                if(body) {
                    res.status(200).send();
                }else{
                    res.status(500).send(UIActionsFactory.getUIActionsObject('Reset Password Failed', 'Banner', 'There was an error reseting your password.'));
                }
            });
        }.bind(this));

        return this.router;
    }
}
module.exports = ResetPasswordRouter;