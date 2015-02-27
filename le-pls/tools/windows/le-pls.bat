echo off
set cmd=%1

if "%1" == "compile" (

    echo compiling using shared GlobalAuth ...

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\bodcdevvint209.dev.lattice.local.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        clean compile

) else if "%1" == "verify" (

    echo running clean verify using shared GlobalAuth ...

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\bodcdevvint209.dev.lattice.local.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        -DargLine="" ^
        clean verify

) else if "%1" == "run" (

    echo running clean verify using shared GlobalAuth ...

    mvn -Pfunctional ^
        -Djavax.net.ssl.trustStore=certificates\bodcdevvint209.dev.lattice.local.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        jetty:run

) else if "%1" == "compile-local" (

    echo compiling using localhost GlobalAuth ...

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        -Dcertificate.name=localhost.jks ^
        -Dglobalauth.base.url=https://localhost/Bard ^
        clean compile

) else if "%1" == "verify-local" (

    echo running clean verify using localhost GlobalAuth ...

     mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        -Dcertificate.name=localhost.jks ^
        -Dglobalauth.base.url=https://localhost/Bard ^
        -DargLine="" ^
        clean verify

) else if "%1" == "run-local" (

    echo running clean verify using localhost GlobalAuth ...

    mvn -Pfunctional ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        jetty:run

) else (

    echo unknown command: %
    echo in ledp/le-pls directory run: tools\windows\le-pls.bat {command}
    echo currently available {command} are: compile, verify, run, compile-local, verify-local, run-local

)