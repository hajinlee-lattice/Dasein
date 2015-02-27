echo off
set cmd=%1

if "%1" == "compile" (

    echo compiling using shared GlobalAuth ...

    cd ..\..

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\bodcdevvint57.dev.lattice.local.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        clean compile

    cd tools\windows

) else if "%1" == "verify" (

    echo running clean verify using shared GlobalAuth ...

    cd ..\..

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\bodcdevvint57.dev.lattice.local.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        -DargLine="" ^
        clean verify

    cd tools\windows

) else if "%1" == "run" (

    echo running clean verify using shared GlobalAuth ...

    cd ..\..

    mvn -Pfunctional ^
        -Djavax.net.ssl.trustStore=certificates\bodcdevvint57.dev.lattice.local.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows ^
        jetty:run

    cd tools\windows

) else if "%1" == "compile-local" (

    echo compiling using localhost GlobalAuth ...

    cd ..\..

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        -Dcertificate.name=localhost.jks ^
        -Dglobalauth.base.url=https://localhost/Bard ^
        clean compile

    cd tools\windows

) else if "%1" == "verify-local" (

    echo running clean verify using localhost GlobalAuth ...

    cd ..\..

    mvn -Pfunctional -Pgenerate ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        -Dcertificate.name=localhost.jks ^
        -Dglobalauth.base.url=https://localhost/Bard ^
        -DargLine="" ^
        clean verify ^

    cd tools\windows

) else if "%1" == "run-local" (

    echo running clean verify using localhost GlobalAuth ...

    cd ..\..

    mvn -Pfunctional ^
        -Djavax.net.ssl.trustStore=certificates\localhost.jks ^
        -DPLS_PROPDIR=conf\env\dev-windows-localGA ^
        jetty:run

    cd tools\windows

) else (

    echo unknown command: %1

)