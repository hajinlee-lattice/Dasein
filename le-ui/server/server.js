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
                console.log(err);
            }
        } 

        // set up view engine for handlebars
        this.app.engine('.html', exphbs({ extname: '.html' }));
        this.app.set('view engine', '.html');
        this.app.set('views', options.APP_ROOT);

        //process.on('uncaughtException', err => app.close());
        //process.on('SIGTERM', err => app.close());
        process.on('ECONNRESET', err => { 
            console.log('ECONNRESET', err);
        });

        this.setMiddleware();
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
            this.app.use(morgan('combined', { stream: accessLogStream }));
            this.app.use(morgan('dev', { 
                skip: function (req, res) { return res.statusCode < 400 } 
            }));
        } catch(err) {
            console.log(err);
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
    useApiProxy(API_URL, API_PATH) {
        if (API_URL) {
            this.API_PATH = API_PATH = API_PATH || '/pls';

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
                        console.log(err);
                    }
                });
            } catch (err) {
                console.log(err);
            }
        }
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
                        this.express.static(dir + route.folders[folder], { maxAge: 31557600 })
                    );
                });
            }

            console.log(
                '> PATH:\t'+route.path.replace(this.options.SRC_PATH,""),
                '[ '+displayString+' ]'
            );

            displayString = '';
            // users will see the desired render page when entering these routes
            if (route.pages) {
                Object.keys(route.pages).forEach(page => {
                    //console.log('\t'+route.pages[page]+'\t->',page);
                    this.app.get(
                        page, 
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
                env: 'Lattice Engines'
            });
        });
    }

    start() {
        const options = this.options;

        console.log('> SERVER OPTIONS:');

        Object.keys(options).forEach(key => {
            var value = options[key];

            console.log('\t' + key + ':\t' + value + ' (' + typeof value + ')');
        });

        console.log('> REDIRECT:', this.API_PATH, ' -> ', options.API_URL);

        if (this.httpServer) {
            this.httpServer.listen(options.HTTP_PORT, () => { console.log('> LISTENING: http://localhost:' + options.HTTP_PORT); });
        }

        if (this.httpsServer) {
            this.httpsServer.listen(options.HTTPS_PORT, () => { console.log('> LISTENING: https://localhost:' + options.HTTPS_PORT); });
        }
    }
}

module.exports = Server;