"use strict";

/*
                Lattice Engines Express Server
    See Gruntfile.js to define LOCAL environment variables
    See /conf/env for setting SERVER environment variables
*/

const path      = require('path');
const exphbs    = require('express-handlebars');
const request   = require('request');
const morgan    = require('morgan');
const fs        = require('graceful-fs');
const helmet    = require('helmet');
const compress  = require('compression');
const chalk     = require('chalk');
/*
const proxy     = require('express-http-proxy');
const session   = require('express-session');
*/
const DateUtil = require('./utilities/DateUtil');

class Server {
    constructor(express, app, options) {
        this.options = options;
        this.express = express;
        this.app = app;

        if (options.config.protocols.http) {
            const http = require('http');

            http.globalAgent.maxSockets = Infinity;
            this.httpServer = http.createServer(this.app);
        }

        if (options.config.protocols.https) {
            try {
                const https = require('https');
                const httpsKey = fs.readFileSync(options.config.HTTPS_KEY, 'utf8');
                const httpsCert = fs.readFileSync(options.config.HTTPS_CRT, 'utf8');
                const credentials = {
                    cert: httpsCert
                };

                options.config.HTTPS_KEY  ? credentials.key = httpsKey : null;
                options.config.HTTPS_PASS ? credentials.passphrase = options.config.HTTPS_PASS : null;

                https.globalAgent.maxSockets = Infinity;
                this.httpsServer = https.createServer(credentials, this.app);
            } catch(err) {
                console.log(chalk.red(DateUtil.getTimeStamp() + ':httpserror>'), err);
            }
        } 

        // order matters
        this.setRenderer();
        this.setMiddleware();
        options.config.WHITELIST ? this.trustProxy(options.config.WHITELIST) : null;
        options.config.LOGGING ? this.startLogging(options.config.LOGGING) : null;
        options.routes ? this.setAppRoutes(options.routes) : null;
        this.setProxies();
        this.setDefaultRoutes(options.config.NODE_ENV);
    }

    setRenderer() {
        // set up view engine for handlebars
        this.app.engine('.html', exphbs({ extname: '.html' }));
        this.app.set('view engine', '.html');
        this.app.set('views', this.options.config.APP_ROOT);
    }

    setMiddleware() {
        // enable gzip compression
        this.app.use(compress());

        // helmet enables/modifies/removes http headers for security concerns
        this.app.use(helmet());


        // default cookie behavior - favors security
        /* don't need this yet
        this.app.use(session({
            name: 'sessionId',
            secret: 'LEs3Cur1ty',
            cookie: {
                secure: true,
                expires: new Date(Date.now() + 60 * 60 * 1000) // 1 hour
            }
        }));
        */
    }

    startLogging(log_path) {
        let logDirectory = log_path;

        try {
            const accessLogStream = fs.createWriteStream(logDirectory + '/' + this.options.name + '_access.log', {
                flags: 'a'
            });

            const map = {
                default:':datetime> :method :url :status :response-time ms - :res[content-length]',
                verbose:':utctime :remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent"'
            };

            morgan.token("datetime", function getDateTime() {
                return DateUtil.getTimeStamp();
            });

            morgan.token("utctime", function getDateTime() {
                return new Date().getTime();
            });

            this.app.use(morgan(
                map[this.options.config.LOGGING_LEVEL],
                {
                    stream: accessLogStream
                }
            ));

            this.app.use(morgan(
                map['default'],
                {
                    skip: function (req, res) {
                        return res.statusCode < 400;
                    }
                }
            ));
        } catch(err) {
            console.log(chalk.red(DateUtil.getTimeStamp() + ':logging>') + err);
        }
    }

    // trust the load balancer/proxy in production
    trustProxy(WHITELIST) {
        this.app.set('trust proxy', ip => {
            const ips = WHITELIST.split(',');

            ips.forEach(current_ip => {
                if (current_ip === ip) {
                    return true;
                }
            });

            return false;
        });
    }

    setProxies() {
        const options = this.options;

        for (let path in options.config.proxies) {
            const proxy = options.config.proxies[path];
            switch (proxy.type) {
                case 'file_pipe':
                    this.createFileProxy(options.API_URL, proxy.local_path, proxy.remote_path);
                    break;
                case 'pipe':
                    this.createApiProxy(proxy.remote_host, proxy.local_path, proxy.remote_path);
                    break;
                default:
                    // throw error invalid configuration?
                    break;
            }
        }
    }

    // forward API requests for dev
    createApiProxy(API_URL, API_LOCAL_PATH, API_PATH) {
        // check if internal IP

        if (API_URL) {
            API_PATH = API_PATH || '/pls';

            console.log(chalk.white('>') + ' API PROXY:', API_LOCAL_PATH, ' -> ', API_URL+API_PATH);

            try {
                this.app.use(API_LOCAL_PATH, (req, res) => {
                    const reqUrl = (req.url[1] === '?') ? req.url.substring(1) : req.url;
                    const url = API_URL + API_PATH + reqUrl;
                    let r = null;

                    try {
                        if (req.method === 'POST') {
                            r = request.post({
                                uri: url,
                                json: req.body
                            });

                            // prevent large-ish files from timing out on upload
                            res.setTimeout(1800000);
                        } else {
                            r = request(url);
                        }

                        req.pipe(r).pipe(res);
                    } catch(err) {
                        console.log(chalk.red(DateUtil.getTimeStamp() + ':apiproxy>') + err);
                    }
                });
            } catch (err) {
                console.log(chalk.red(DateUtil.getTimeStamp() + ':apiroute>') + err);
            }
        }
    }

    // this is needed so that the browser can download files
    // that need to be hidden behind the Authorization token
    createFileProxy(API_URL, API_PATH, PATH) {
        if (API_URL) {
            API_PATH = API_PATH || '/files',
                PATH = PATH || '/pls';

            console.log(chalk.white('>') + ' FILE PROXY:', API_PATH, ' -> ', API_URL+PATH);

            this.app.use(API_PATH, (req, res) => {
                // urls heading to /files/* will go to /pls/* with Auth token
                const url = API_URL + PATH + req.url;

                try {
                    let r = request(url);

                    if (req.query) {
                        if (req.query.Authorization) {
                            // Since the token was in the URL, +'s got converted to spaces
                            let token = req.query.Authorization.replace(/ /g,'+');
                            req.headers["Authorization"] = token || '';
                        }

                        if (req.query.TenantId) {
                            let tenant = req.query.TenantId;
                            req.headers["TenantId"] = tenant || '';
                        }
                    }

                    req.pipe(r).pipe(res);
                } catch(err) {
                    console.log(chalk.red(DateUtil.getTimeStamp() + ':fileproxy>') + err);
                }
            });
        }
    }

    setAppRoutes(routes) {
        routes.forEach(route => {
            const dir = this.options.config.APP_ROOT + route.path;
            var displayString = '';
            // set up the static routes for app files
            if (route.folders) {
                Object.keys(route.folders).forEach(folder => {
                    displayString += (displayString ? ', ' : '') + route.folders[folder];
                    this.app.use(
                        folder,
                        this.express.static(dir + route.folders[folder]) //, { maxAge: 3600000 })
                    );
                });
            }

            console.log(
                chalk.white('>') + ' PATH:\t'+route.path.replace(this.options.SRC_PATH,""),
                '[ '+displayString+' ]'
            );

            displayString = '';
            // users will see the desired render page when entering these routes
            if (route.pages) {
                const html5mode = route.html5mode === true;
                Object.keys(route.pages).forEach(page => {
                    //console.log('\t'+route.pages[page]+'\t->',page);
                    // if !internal ip && page == '/admin'
                    this.app.get(
                        page + (html5mode ? '*' : ''),
                        (req, res) => res.render(dir + '/' + route.pages[page])
                    );
                });
            }
        });
    }

    setDefaultRoutes(NODE_ENV) {
        // catch 404 and forwarding to error handler
        this.app.use((req, res, next) => {
            const err = new Error('Not Found');
            err.status = 404;
            next(err);
        });

        // print stack trace for dev environment
        if (NODE_ENV != 'production') {
            this.app.use((err, req, res, next) => {
                res.status(err.status || 500);
                res.render('server/error', {
                    options: this.options,
                    url: req.originalUrl,
                    status: err.status,
                    message: err.message,
                    error: err,
                    env: NODE_ENV
                });
            });
        }

        // no stacktraces leaked to user for production
        this.app.use((err, req, res, next) => {
            res.status(err.status || 500);
            res.render('server/error', {
                url: req.originalUrl,
                status: err.status,
                message: err.message,
                error: 'Please contact the administrator if the problem persists.',
                env: false
            });
        });
    }

    start() {
        const options = this.options.config;
        const line = '-------------------------------------------------------------------------------';

        //if (options.NODE_ENV == 'development') {
        //    console.log(chalk.yellow('> TLS:') + ' Allow Unauthorized in Development Mode')
            process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';
        //}

        console.log(chalk.white('>') + ' SERVER SETTINGS:');

        console.log(JSON.stringify(options, null, 4));
        // Object.keys(options).forEach(key => {
        //     const value = options[key],
        //         ignore = ['TIMESTAMP'];

        //     if (ignore.indexOf(key) < 0) {
        //         console.log(chalk.white('\t' + key + ':\t') + value + ' (' + typeof value + ')');
        //     }
        // });


        if (this.httpServer) {
            this.httpServer.listen(options.protocols.http, () => {
                console.log(chalk.green(options.TIMESTAMP + '>') + ' LISTENING: http://localhost:' + options.protocols.http);
            });
        }

        if (this.httpsServer) {
            this.httpsServer.listen(options.protocols.https, () => {
                console.log(chalk.green(options.TIMESTAMP + '>') + ' LISTENING: https://localhost:' + options.protocols.https);
                console.log(line);
            });
        }

    }
}

module.exports = Server;
