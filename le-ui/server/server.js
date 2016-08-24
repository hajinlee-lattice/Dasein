"use strict";

/*
            Lattice Engines Express Server
    See Gruntfile.js to define environment variables
*/

const path      = require('path');
const exphbs    = require('express-handlebars');
const request   = require('request');
const morgan    = require('morgan');
const fs        = require('fs');
const helmet    = require('helmet');
//const session   = require('express-session');
const compress  = require('compression');
const chalk     = require('chalk');

class Server {
    constructor(express, app, options) {
        this.options = options;
        this.express = express;
        this.app = app;

        if (options.HTTP_PORT) {
            const http = require('http');

            http.globalAgent.maxSockets = Infinity;
            this.httpServer = http.createServer(this.app);
        }

        if (options.HTTPS_PORT) {
            try {
                const https = require('https');
                const httpsKey = fs.readFileSync(options.HTTPS_KEY, 'utf8');
                const httpsCert = fs.readFileSync(options.HTTPS_CRT, 'utf8');
                const credentials = {
                    cert: httpsCert
                };
                
                options.HTTPS_KEY  ? credentials.key = httpsKey : null; 
                options.HTTPS_PASS ? credentials.passphrase = options.HTTPS_PASS : null; 

                https.globalAgent.maxSockets = Infinity;
                this.httpsServer = https.createServer(credentials, this.app);
            } catch(err) {
                console.log(chalk.red(this.getTimestamp() + ':httpserror>'), err);
            }
        } 

        // set up view engine for handlebars
        this.app.engine('.html', exphbs({ extname: '.html' }));
        this.app.set('view engine', '.html');
        this.app.set('views', options.APP_ROOT);

        this.setMiddleware();

        process.on('uncaughtException', err => {
            console.log(chalk.red(this.getTimestamp() + ':uncaughtException>'), err);
            //this.app.close();
        });
        
        process.on('SIGTERM', err => {
            console.log(chalk.red(this.getTimestamp() + ':SIGTERM>'), err);
        });
        
        process.on('ECONNRESET', err => { 
            console.log(chalk.red(this.getTimestamp() + ':ECONNRESET>'), err);
        });
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
            var accessLogStream = fs.createWriteStream(logDirectory + '/le-ui_access.log', {flags: 'a'})
            
            morgan.token("datetime", function getDateTime() {
                const ts = new Date();
                
                return (ts.getMonth() + 1) + '/' + ts.getDate() + '/' + ts.getFullYear() + ' ' +
                        ts.getHours() + ':' + ts.getMinutes() + ':' + ts.getSeconds();
            });

            morgan.token("utctime", function getDateTime() {
                return new Date().getTime();
            });
            
            this.app.use(morgan(
                ':utctime :remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent"', 
                { 
                    stream: accessLogStream 
                }
            ));

            this.app.use(morgan(
                ':datetime> :method :url :status :response-time ms - :res[content-length]', 
                { 
                    skip: function (req, res) { 
                        return res.statusCode < 400
                    } 
                }
            ));
        } catch(err) {
            console.log(chalk.red(this.getTimestamp() + ':logging>') + err);
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

    // forward API requests for dev
    createApiProxy(API_URL, API_PATH) {
        if (API_URL) {
            var API_PATH = API_PATH || '/pls';
            
            console.log(chalk.white('>') + ' API PROXY:', API_PATH, ' -> ', API_URL+API_PATH);

            try {
                this.app.use(API_PATH, (req, res) => {
                    const url = API_URL + API_PATH + req.url;
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
                        console.log(chalk.red(this.getTimestamp() + ':apiproxy>') + err);
                    }
                });
            } catch (err) {
                console.log(chalk.red(this.getTimestamp() + ':apiroute>') + err);
            }
        }
    }

    // this is needed so that the browser can download files
    // that need to be hidden behind the Authorization token
    createFileProxy(API_URL, API_PATH, PATH) {
        if (API_URL) {
            var API_PATH = API_PATH || '/pls',
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
                    console.log(chalk.red(this.getTimestamp() + ':fileproxy>') + err);
                }
            });
        }
    }

    getTimestamp() {
        const ts = new Date();
        
        return (ts.getMonth() + 1) + '/' + ts.getDate() + '/' + ts.getFullYear() + ' ' +
                ts.getHours() + ':' + ts.getMinutes() + ':' + ts.getSeconds();
    }

    setAppRoutes(routes) {
        routes.forEach(route => {
            const dir = this.options.APP_ROOT + route.path;
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
                var html5mode = route.html5mode == true;
                Object.keys(route.pages).forEach(page => {
                    //console.log('\t'+route.pages[page]+'\t->',page);
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
        const options = this.options;
        const line = '-------------------------------------------------------------------------------';

        //if (options.NODE_ENV == 'development') {
        //    console.log(chalk.yellow('> TLS:') + ' Allow Unauthorized in Development Mode')
        process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';
        //}

        console.log(chalk.white('>') + ' SERVER SETTINGS:');

        Object.keys(options).forEach(key => {
            var value = options[key],
                ignore = ['TIMESTAMP'];

            if (ignore.indexOf(key) < 0) {
                console.log(chalk.white('\t' + key + ':\t') + value + ' (' + typeof value + ')');
            }
        });

        console.log(line);

        if (this.httpServer) {
            this.httpServer.listen(options.HTTP_PORT, () => { 
                console.log(chalk.green(options.TIMESTAMP + '>') + ' LISTENING: http://localhost:' + options.HTTP_PORT); 
            });
        }

        if (this.httpsServer) {
            this.httpsServer.listen(options.HTTPS_PORT, () => { 
                console.log(chalk.green(options.TIMESTAMP + '>') + ' LISTENING: https://localhost:' + options.HTTPS_PORT);
                console.log(line);
            });
        }

    }
}

module.exports = Server;