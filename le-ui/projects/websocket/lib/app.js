(function(){
    var stompClient = null;
    var reconnecting = false;
    var connected = false;

    function setConnected(val) {
        connected = val;
        $("#connect").prop("disabled", val);
        $("#disconnect").prop("disabled", !val);
        if (val) {
            $("#conversation").show();
        }
        else {
            $("#conversation").hide();
        }
    }

    function connect() {
        console.log('Connecting webscoket');
        var socket = new SockJS('/pls/sockjs-endpoint');
        stompClient = Stomp.over(socket);
        stompClient.connect({}, function (frame) {
            setConnected(true);
            reconnecting = false;
            showMessage("Connected.")
            console.log('Connected: ' + frame);
            stompClient.subscribe('/topic/greetings', function (greeting) {
                showMessage(JSON.parse(greeting.body).content);
            });
        }, function(e) {
            if (!reconnecting) {
                connected = false;
                reconnecting = true;
                showMessage(e);
                showMessage("Reconnecting ...");
                if (stompClient !== null) {
                    stompClient.disconnect();
                }
            }
            if (!connected) {
                connect();
            }
        });
    }

    function disconnect() {
        if (stompClient !== null) {
            stompClient.disconnect();
        }
        setConnected(false);
        $("#messages").html("");
        console.log("Disconnected");
    }

    function sendToken() {
        var token = $("#token").val();
        console.log('Sending token ' + token + ' to authenticate the webscoket session.');
        stompClient.send("/app/authenticate", {}, JSON.stringify({'Token': token}));
    }

    function showMessage(message) {
        $("#messages").append("<tr><td>" + message + "</td></tr>");
    }

    $(function () {
        $("form").on('submit', function (e) {
            e.preventDefault();
        });
        $( "#connect" ).click(function() { connect(); });
        $( "#disconnect" ).click(function() { disconnect(); });
        $( "#send" ).click(function() { sendToken(); });
    })

}).call(this);

