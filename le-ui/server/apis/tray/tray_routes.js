const GraphQLParser = require('../../parsers/graphql-parser');
const Queries = require('../queries');
const UIActionsFactory = require('../uiactions-factory');
/**
 * Routing for tray's apis
 * End point for UI defined here
 */

class TrayRouter {
    constructor(express, app, bodyParser, chalk, API_URL, PATH, request, proxies, masterAuthorizationToken) {
        this.router = express.Router();
        this.chalk = chalk;
        this.API_URL = API_URL;
        this.PATH = PATH;
        this.request = request;
        this.proxies = proxies;
        this.masterAuthorizationToken = masterAuthorizationToken;
        app.use(bodyParser.json());
    }

    getApiOptions(req, useUserAccessToken) {

        var authorization = (req.headers && req.headers.useraccesstoken && useUserAccessToken == true) ?
            req.headers.useraccesstoken :
            this.masterAuthorizationToken;

        const options = {
            url: this.API_URL + this.PATH,
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${authorization}`
            }
        };

        return options;
    }

    getEphemeralApiOptions(req, useUserAccessToken) {

        var authorization = (req.headers && req.headers.useraccesstoken && useUserAccessToken == true) ?
            req.headers.useraccesstoken :
            this.masterAuthorizationToken;

        const options = {
            url: "https://api.tray.io/v1/artisan/connectors/marketo/2.10/ephemeral",
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
                    res.send(UIActionsFactory.getUIActionsObject('Unauthorized', 'Notice', 'Error'))
                }
            });
        }.bind(this));

        // define the route to verify if the user exists otherwise it is going to be created
        this.router.get('/userdocument', function (req, res) {
            var plsUrl = this.proxies['/pls']['remote_host'];
            var url = '/pls/dropbox/summary';
            const options = {
                url: plsUrl + url,
                method: 'GET',
                headers: {
                    'Authorization': req.headers.authorization ? req.headers.authorization : ''
                }
            };

            try {
                this.request(options, function (error, response, body) {
                    var jsonBody = JSON.parse(body);
                    if (error || !jsonBody || !jsonBody.DropBox) {
                        res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                        return;
                    }
                    req.url = "/user";
                    req.method = 'GET'
                    req.query.userName = jsonBody.DropBox;
                    this.router.handle(req, res);
                }.bind(this));
            } catch (err) {
                console.log(
                    this.chalk.red(DateUtil.getTimeStamp() + ":TRAY PROXY ") + err
                );
            }
        }.bind(this));

        // define the route to verify if the user exists otherwise it is going to be created
        this.router.get('/user', function (req, res) {
            console.log('USERNAME ', req.query.userName);

            try {
                var options = this.getApiOptions(req);
                options.json = Queries.getUserQuery(req.query.userName);
                this.request(options, function (error, response, body) {
                    if (error || !body.data) {
                        res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                        return;
                    }
                    let userInfo = GraphQLParser.getUserInfo(body.data);
                    if (userInfo == null) {
                        req.url = "/user";
                        req.method = 'POST'
                        req.body = { userName: req.query.userName, validated: true };
                        this.router.handle(req, res);
                    } else {
                        req.userInfo = userInfo;
                        req.url = "/authorize?userId=" + userInfo.id;
                        req.method = 'POST';
                        req.query.userId = userInfo.id;
                        this.router.handle(req, res);
                    }
                }.bind(this));
            } catch (err) {
                console.log(
                    this.chalk.red(DateUtil.getTimeStamp() + ":TRAY PROXY ") + err
                );
            }
        }.bind(this));

        /* 
            CREATE USER
        */
        this.router.post('/user', function (req, res) {
            if (req.body.validated === true) {
                let options = this.getApiOptions(req);
                options.json = Queries.getCreateUserMutation(req.body.userName);
                this.request(options, function (error, response, body) {
                    if (error || !body.data) {
                        res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                        return;
                    }
                    let userInfo = GraphQLParser.getUserInfo(body.data);
                    req.userInfo = userInfo;
                    req.url = "/authorize?userId=" + userInfo.id;
                    req.method = 'POST';
                    req.query.userId = userInfo.id;
                    this.router.handle(req, res);
                }.bind(this));
            } else {
                res.send(UIActionsFactory.getUIActionsObject('Not validated', 'Notice', 'Error'));
            }

        }.bind(this));

        /* 
            GENERATE ACCESS TOKEN FOR TENANT
        */
        this.router.post('/authorize', function (req, res) {
            let options = this.getApiOptions(req);
            options.json = Queries.getAuthorizeUserMutation(req.query.userId);
            this.request(options, function(error, response, body){
                if (error || !body.data) {
                    res.send(UIActionsFactory.getUIActionsObject(error || body.errors, 'Notice', 'Error'));
                    return;
                }
                if (req.userInfo == null) {
                    let accessToken = GraphQLParser.getAuthorizeInfo(body.data);
                    res.send(accessToken);
                } else {
                    res.send(GraphQLParser.getUserDocument(req.userInfo, GraphQLParser.getAuthorizeInfo(body.data)));
                }

            });
        }.bind(this));

        this.router.get('/authorizationcode', function(req, res){
            console.log('USERNAME ', req.query.userId);
            let options = this.getApiOptions(req);
            options.json = Queries.getAuthorizationCodeMutation(req.query.userId);
            this.request(options, function(error, response, body){
                if (error || !body.data) {
                    res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                    return;
                }
                let authorizationCode = GraphQLParser.getAuthorizationCodeInfo(body.data);
                res.send(authorizationCode);
            });
        }.bind(this));

        this.router.get('/solutionInstances/:id', function(req, res){
            var solutionInstanceId = req.params.id;
            let options = this.getApiOptions(req, true);
            options.json = Queries.getSolutionInstanceByIdQuery(solutionInstanceId);
            this.request(options, function(error, response, body){
                if (error || !body.data) {
                    res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                    return;
                }
                res.send(GraphQLParser.getSolutionInstance(body.data));
                
            });
        }.bind(this));

        this.router.get('/solutions', function(req, res){
            var tag = req.query.tag;
            let options = this.getApiOptions(req);
            options.json = Queries.getSolutionsByTagQuery(tag);
            this.request(options, function(error, response, body){
                if (error || !body.data) {
                    res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                    return;
                }
                let solutionInfo = GraphQLParser.getSolutionInfo(body.data);
                res.send(solutionInfo);
            });
        }.bind(this));

        this.router.post('/solutionInstances', function(req, res){
            var solutionId = req.query.solutionId;
            var instanceName = req.query.instanceName;
            let options = this.getApiOptions(req);
            options.json = Queries.getCreateSolutionInstanceMutation(solutionId, instanceName);
            this.request(options, function(error, response, body){
                if (error || !body.data) {
                    res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                    return;
                }
                let solutionInfo = GraphQLParser.getSolutionInstanceInfo(body.data);
                res.send(solutionInfo);
            });
        }.bind(this));

        this.router.get('/solutionconfiguration', function(req, res, next) {
            /*
                Get solution to instantiate 
            */
            let options = this.getApiOptions(req);
            options.json = Queries.getSolutionsByTagQuery(req.query.tag);
            this.request(options, function(error, response, body){
                if (error || !body.data) {
                    res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                    return;
                }
                let solutionInfo = GraphQLParser.getSolutionInfo(body.data);
                if (!solutionInfo) {
                    res.send(UIActionsFactory.getUIActionsObject('Could not find solution with tag ' + req.query.tag, 'Notice', 'Error'));
                    return;
                }
                req.solutionId = solutionInfo.id;
                console.log("Solution to instantiate: " + req.solutionId);
                next();
            });
        }.bind(this), function(req, res, next) {
            /*
                Get solution instance id
            */
            let instanceName = req.query.instanceName;
            let solutionId = req.solutionId;
            let options = this.getApiOptions(req, true);
            options.json = Queries.getCreateSolutionInstanceMutation(solutionId, instanceName);
            this.request(options, function(error, response, body){
                if (error || !body.data) {
                    res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                    return;
                }
                let solutionInstanceInfo = GraphQLParser.getSolutionInstanceInfo(body.data);
                req.solutionInstanceId = solutionInstanceInfo.id;
                next();
            });
        }.bind(this), function(req, res) {
            /*
                Get authorization code
            */
            var solutionInstanceId= req.solutionInstanceId;
            let options = this.getApiOptions(req);
            options.json = Queries.getAuthorizationCodeMutation(req.query.userId);
            this.request(options, function(error, response, body){
                if (error) {
                    res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                    return;
                }
                let authorizationCode = GraphQLParser.getAuthorizationCodeInfo(body.data);
                console.log(authorizationCode);
                let solutionConfiguration = GraphQLParser.getSolutionConfigurationInfo(solutionInstanceId, authorizationCode);
                res.send(solutionConfiguration);
            });
        }.bind(this));


        this.router.put('/solutionInstances/:id', function(req, res){
            var solutionInstanceId = req.params.id;
            var solutionInstanceName = req.body.solutionInstanceName;
            var authValues = req.body.authValues;
            console.log(JSON.stringify(authValues))
            let options = this.getApiOptions(req, true);
            options.json = Queries.updateSolutionInstanceQuery(solutionInstanceId, solutionInstanceName, authValues);
            console.log(JSON.stringify(options.json));
            this.request(options, function(error, response, body){
                if (error || !body.data) {
                    res.send(UIActionsFactory.getUIActionsObject(error, 'Notice', 'Error'));
                    return;
                }
                res.send(GraphQLParser.getUpdateSolutionInstanceInfo(body.data))
                
            });
        }.bind(this));


        /*
            EPHEMERAL API
        */
        this.router.get('/marketo/staticlists', function(req, res){
            var authenticationId = req.query.trayAuthenticationId || '';
            var programName = req.query.programName || '';
            let options = this.getEphemeralApiOptions(req, true);
            options.json = Queries.listMarketoStaticLists(authenticationId, programName);
            this.request(options, function(error, response, body){
                var errorMessage = GraphQLParser.getErrorMessage(body);

                if (errorMessage) {
                    console.log("Couldn't retrieve static lists for authenticationId " + authenticationId + ": " + JSON.stringify(body));
                    res.send(UIActionsFactory.getUIActionsObject(errorMessage, 'Banner', 'Error'));
                    return;
                }
                res.send(body);
            });
        }.bind(this));

        this.router.get('/marketo/programs', function(req, res){
            var authenticationId = req.query.trayAuthenticationId || '';
            let options = this.getEphemeralApiOptions(req, true);
            if (req.query.includeDateFilter == true) {
                var date = new Date();
                date.setFullYear(date.getFullYear() - 1);
                options.json = Queries.getMarketoPrograms(authenticationId, date.toISOString());
            } else {
                options.json = Queries.getMarketoPrograms(authenticationId);
            }

            this.request(options, function(error, response, body){
                var errorMessage = GraphQLParser.getErrorMessage(body);

                if (errorMessage) {
                    console.log("Couldn't retrieve program lists for authenticationId " + authenticationId + ": " + JSON.stringify(body));
                    res.send(UIActionsFactory.getUIActionsObject(errorMessage, 'Banner', 'Error'));
                    return;
                }

                if (body.result && body.result.length == 200 && req.query.includeDateFilter == undefined) {
                    console.log('Filter by date for trayAuthenticationId' + authenticationId);
                    req.url = "/marketo/programs";
                    req.method = 'GET'
                    req.query.trayAuthenticationId = req.query.trayAuthenticationId;
                    req.query.includeDateFilter = true;
                    req.headers.useraccesstoken = req.headers.useraccesstoken;
                    this.router.handle(req, res);
                } else {
                    res.send(body);
                }
            }.bind(this));
        }.bind(this));

        this.router.get('/facebook/audiences', function(req, res){
            res.send({
                success: true,
                errors: [],
                requestId: "foo",
                warnings: [],
                result: []
            });
        }.bind(this));

        this.router.get('/linkedin/audiences', function(req, res){
            res.send({
                success: true,
                errors: [],
                requestId: "foo",
                warnings: [],
                result: []
            });
        }.bind(this));

        return this.router;
    }

}
module.exports = TrayRouter;