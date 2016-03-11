"use strict";

/*
            Lattice Engines Express Server
    See Gruntfile.js to define environment variables
*/

const rotator   = require('file-stream-rotator');
const path      = require('path');
const exphbs    = require('express-handlebars');
const bodyParser = require('body-parser');
const request   = require('request');
const morgan    = require('morgan');
const fs        = require('fs');
const busboy    = require('busboy');

class Server {
    constructor(express, app, options) {
        this.options = options;
        this.express = express;
        this.app = app;
/*
        this.app.use(
            bodyParser({ 
                keepExtensions: true, 
                uploadDir: "uploads" 
            })
        );                     
*/
        
        //busboy.extend(this.app)

        // set up view engine for handlebars
        this.app.engine('.html', exphbs({ extname: '.html' }));
        this.app.set('view engine', '.html');
        this.app.set('views', options.root);

        //process.on('uncaughtException', err => this.app.close());
        //process.on('SIGTERM', err => this.app.close());
    }

    startLogging(log_path) {
        let logDirectory = __dirname + log_path;

        let accessLogStream = rotator.getStream({
            date_format: 'YYYYMMDD',
            filename: logDirectory + '/access-%DATE%.log',
            frequency: 'daily',
            verbose: false
        });

        this.app.use(morgan('combined', { stream: accessLogStream }));
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
    useApiProxy(API_URL) {
        if (API_URL) {
            try {
                this.app.use('/pls', (req, res) => {
                    const url = API_URL + '/pls' + req.url;
                    let r = null;

                    try {
                        if (req.method === 'POST') {
                            console.log(req);
                            r = request.post({ 
                                uri: url, 
                                json: req.body
                            });

                            res.setTimeout(900000);
                        } else {
                            r = request(url);
                        }

                        req.pipe(r).pipe(res);
                    } catch(err) {
                        console.log(err.msg);
                    }
                });
            } catch (err) {
                console.log(err.msg);
            }
        }
    }

    setAppRoutes(routes) {
        console.log('> WEB ROOT:',this.options.root);
        routes.forEach(route => {
            const dir = this.options.root + route.path;
            var displayString = '';
            console.log('> PATH:\t'+route.path);
            // set up the static routes for app files
            if (route.folders) {
                Object.keys(route.folders).forEach(folder => {
                    displayString += (displayString ? ', ' : '') + route.folders[folder];
                    this.app.use(
                        folder, 
                        this.express.static(dir + route.folders[folder])
                    );
                });
            }

            console.log('\t[ '+displayString+' ]');

            displayString = '';
            // users will see the desired render page when entering these routes
            if (route.pages) {
                Object.keys(route.pages).forEach(page => {
                    console.log('\t'+route.pages[page]+'\t->',page);
                    this.app.get(
                        page, 
                        (req, res) => res.render(dir + '/' + route.pages[page])
                    );
                });
            }
        });
    }

    setDefaultRoutes(ENV) {
        // catch 404 and forwarding to error handler
        this.app.use((req, res, next) => {
            const err = new Error('Not Found');
            err.status = 404;
            next(err);
        });

        // print stack trace for dev environment
        if (ENV === 'development') {
            this.app.use((err, req, res, next) => {
                res.status(err.status || 500);
                res.render('./server/error', {
                    message: err.message,
                    error: err,
                    env: ENV
                });
            });
        }

        // no stacktraces leaked to user for production
        this.app.use((err, req, res, next) => {
            res.status(err.status || 500);
            res.render('./server/error', {
                message: err.message,
                error: 'hidden',
                env: ENV
            });
        });
    }

    start() {
        const options = this.options;
        const server = this.app.listen(options.USE_PORT, () => {
            console.log(
                '> HOST: http://localhost:' + options.USE_PORT + '/' + 
                '\tENV:', options.ENV, '\n' +
                '> API:', options.API_URL
            );
        });
    }
}

module.exports = Server;