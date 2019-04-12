const UIActionsFactory = require('../uiactions-factory');

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
        this.router.put('/', function (req, res) {
            if(!req.query.userName || req.query.userName == null || req.query.userName.trim() == ''){
                res.status(400).send(UIActionsFactory.getUIActionsObject('Reset Password', 'Notice', 'Request missing userName'));
                return;
            }
            try{
                let body = {
                    Username: req.query.userName,
                    HostPort: this.getPlsHost() + ':443',
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

                }
                this.request(options, (error, response, body) => {
                    this.log('Call back ', error, response, body);
                    if(body) {
                        res.status(200).send(UIActionsFactory.getUIActionsObject('Reset Password', 'Banner', 'Password Reset'));
                    }else{
                        res.status(response.statusCode).send(UIActionsFactory.getUIActionsObject('Reset Password', 'Banner', 'Error Password Reset'));
                    }
                });
            }catch(err) {
                res.status(500).send(UIActionsFactory.getUIActionsObject('Reset Password', 'Banner', 'Generic server error'));
            }

        }.bind(this));

        return this.router;
    }
}
module.exports = ResetPasswordRouter;