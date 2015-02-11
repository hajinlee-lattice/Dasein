<!DOCTYPE html>
<html lang="en">
<head runat="server">
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
    <meta http-equiv="X-UA-Compatible" content="IE=9"><!-- This is needed to prevent IE from rendering the site in Compatibility Mode -->
    <link rel="shortcut icon" href="assets/CommonAssets/images/lattice.ico" type="image/x-icon"> 
    <title>Lattice Engines</title>
    <link href="assets/styles/production.css" rel="stylesheet" type="text/css" />
</head>
<body>
    <div id="mainView" class="fillSpace"> 
        <div class="initial-loading-placeholder"> </div>
        <div class="initial-loading-spinner-container">
            <img class="centered" width="80" height="80" src="assets/CommonAssets/images/loading_spinner.gif"/>
        </div>
    </div>
    <div id="notificationCenter"></div>
    <div id="dialogContainer"> </div>
    <!-- 
    This div will be added to the DOM if the browser is IE 
    Use DOMUtil.isIE() to detect internet explorer.
    -->
    <!--[if IE]> <div id="isIE"></div> <![endif]-->
    
    <script>
    if (location.protocol == "http:") {
        window.location = "https:" +location.href.substring(5);
    }
    </script>
    <script src="steal.production.js"></script>
    <script src="production.js"></script>
    <script>
        // Create global variable for Evergage to use
        var _aaq = _aaq || [];
    </script>
</body>
</html>