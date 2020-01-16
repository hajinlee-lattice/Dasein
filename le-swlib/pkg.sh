JARFILE=`ls target/le-swlib-*-shaded.jar`

rm -rf target/swlib || true

for library in 'leadprioritization' 'cdl' 'dcp' 'datacloud' 'modeling' 'scoring'; do
    artifactId="le-serviceflows-${library}"
    java -cp $JARFILE com.latticeengines.swlib.SwlibTool \
            -o config \
            -n ${library} \
            -g com.latticeengines \
            -a ${artifactId} \
            -v "" \
            -s "" \
            -i com.latticeengines.${library}.Initializer \
            -f ""
done
