LE-UI Node/Express Setup:
----------------------------------------------------------------------

    This document will explain how to do the following
        1. Install NodeJS and the accompanied LE-UI "Express" web server
        2. Setup tools and your environment to develop projects on the LE-UI platform


PRE-REQUISITE PROGRAMS
----------------------------------------------------------------------
    
    You must have these programs installed:
    
        Ruby
            sudo apt-get install ruby-full
         
        SASS (needs Ruby)
            sudo su -c "gem install sass"
         
        NodeJS/NPM
            curl -sL https://deb.nodesource.com/setup_4.x | sudo -E bash -
            sudo apt-get install -y nodejs
         
        grunt-cli
            sudo npm install -g grunt-cli

    Instructions are for Linux (Ubuntu) found via Google.  Mac/Windows is similar, use Google


INSTALLATION INSTRUCTIONS
----------------------------------------------------------------------

    1. svn checkout develop/ledp/le-ui
    2. type `npm install` in the le-ui folder to install all dependencies


NPM AND GRUNT COMMANDS
----------------------------------------------------------------------
    
    The web server can be started in different modes, type one of these in the le-ui folder

        'npm run dev' - Compile SASS, Start HTTPD, Use QA Stack A API, Run SASS Sentry in background for css dev
        'npm run devb' - Compile SASS, Start HTTPD, Use QA Stack B API, Run SASS Sentry in background for css dev
        'npm run local' - Compile SASS, Start HTTPD, Use localhost for API, Run SASS Sentry in background for css dev
        'grunt newdev' - Start HTTPD, Use QA Stack A
        'grunt newdevb' - Start HTTPD, Use QA Stack B
        'grunt qa' - Start HTTPD, Use QA Stack A API, Use DIST routes and minified files
        'grunt prod' - Start HTTPD, Use Prod Stack A API, Use DIST routes and minified files
        'grunt proddev' - Start HTTPD, Use Prod Stack A API, Use dev unminified files
         
    Other helpful LE-UI commands

        'npm run build' - Build all projects (compile CSS, minify and concat HTML, minify and concat JavaScript)
            (you can also do commonbuild, lpbuild, pdbuild, loginbuild for specific projects)
        'npm run sass' - Compile SASS for all projects
            (you can also do commonsass, lpsass, pdsass for specific projects) 
        'npm run sentry' - Recompiles SASS if any source .scss files change for all projects 
            (you can also do commonsentry, lpsentry, pdsentry for specific projects)  
        'grunt devmon' - For node development, automatically restarts HTTPD if HTTPD source files change
        'grunt killnode' - on windows, this will kill ghost node processes that npm can (often) spawn, causing EADDRINUSE errors 
         
    Project-specific Commands (do these inside the project's folder)

        'npm install' - Install dependencies for this specific project
        'grunt build' - Compile CSS, minify and concat HTML, minify and concat JavaScript
        'grunt sass' - Compile SASS into CSS
        'grunt sentry' - Run SASS Sentry to watch files and recompile CSS 
        'grunt unit' - Run Karma unit tests for the current project 


TROUBLESHOOTING
----------------------------------------------------------------------

    If you have multiple versions of node installed using nvm, you can switch between them using `nvm use <version>`.  The Express server is tested to work with 4.1.2, so you might want to `nvm install 4.1.2` specifically and then `nvm use 4.1.2` if you are having weird issues during runtime.
    If you can’t connect, make sure the server is running and verify the console.log output makes sense for the environment you are running in.
    If you get an EADDRINUSE error when starting the server, killall node and try again. (Windows?  Use `grunt killnode`)
