const GraphQLParser = require('../parsers/graphql-parser');
const UIActionsFactory = require('./uiactions-factory');
/**
 * Routing for tray's apis
 * End point for UI defined here
 */
class TrayRouter {
    constructor(express, app, bodyParser, chalk, API_URL, PATH, request, proxies) {
        this.router = express.Router();
        this.chalk = chalk;
        this.API_URL = API_URL;
        this.PATH = PATH;
        this.request = request;
        this.proxies = proxies;
        console.log(bodyParser);
        app.use(bodyParser.json());
    }

    getApiOptions(req) {

        var authorization = (req.headers && req.headers.UserAccessToken) ?
            req.headers.UserAccessToken :
            "6cadf407-a686-41be-92e7-36e37c97c1e3";

        const options = {
            url: this.API_URL + this.PATH,
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${authorization}`
            }
        };

        return options;
    }
    createRoutes() {

        console.log('============> TRAY API ========================');
        this.router.use(function timeLog(req, res, next) {
            var plsUrl = this.proxies['/pls']['remote_host'];
            var validationUrl = '/pls/cdl/s3import/template';
            const options = {
                url: plsUrl + validationUrl,
                method: 'GET',
                headers: {
                    'Authorization': req.headers.authorization ? req.headers.authorization : ''
                }
            };

            console.log('Time: ', Date.now(), req.headers, this.proxies);

            this.request(options, function (error, response, body) {
                if (response.statusCode == 200) {
                    next();
                } else {
                    res.send(UIActionsFactory.getUIActionsObject('Unauthorized', 'Notice', 'Error'))
                }
            });
        }.bind(this));

        // define the route to verify if the user exists otherwise it is going to be created
        this.router.get('/user', function (req, res) {
            console.log('REQUEST ', req.headers);
            console.log('USERNAME ', req.query.userName);

            try {
                var options = this.getApiOptions(req);
                options.json = GraphQLParser.getUserQuery(req.query.userName);
                this.request(options, function (error, response, body) {
                    if (error) {
                        res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                        return;
                    }
                    console.log(body.data);
                    let userInfo = GraphQLParser.getUserInfo(body.data);
                    if (userInfo == null) {
                        req.url = "/user";
                        req.method = 'POST'
                        req.body = { userName: req.query.userName, validated: true };
                        this.router.handle(req, res);
                    } else {
                        res.send(userInfo);
                    }

                }.bind(this));
            } catch (err) {
                console.log(
                    this.chalk.red(DateUtil.getTimeStamp() + ":TRAY PROXY ") + err
                );
            }
        }.bind(this));

        this.router.post('/user', function (req, res) {
            console.log('USER CREATION', req.body);
            if (req.body.validated === true) {

                let options = this.getApiOptions(req);
                options.json = GraphQLParser.getCreateUserQuery(req.query.userName);
                this.request(options, function (error, response, body) {
                    if (error) {
                        res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                        return;
                    }
                    let userInfo = GraphQLParser.getUserInfo(body.data);
                    res.send(userInfo);
                }.bind(this));
            } else {
                res.send(UIActionsFactory.getUIActionsObject('Not validated', 'Notice', 'Error'));
            }

        }.bind(this));
        return this.router;
    }
}
module.exports = TrayRouter;