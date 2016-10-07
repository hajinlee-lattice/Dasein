#!/bin/bash

echo -n "It needs 20GB disk space to setup account master on local. Do you want to continue? (Y/N)"
read option
if [ $option = "Y" ] || [ $option = "y" ]
then
	hadoop fs -rm -r -skipTrash /Pods/Default/Services/PropData/Sources/AccountMaster

	hadoop distcp hdfs://bodcdevvhdp185.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/ReducedAccountMaster/Schema/2016-10-06_17-17-47_UTC hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMaster/Schema/2016-10-06_17-17-47_UTC || hadoop distcp hdfs://bodcdevvhdp104.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/ReducedAccountMaster/Schema/2016-10-06_17-17-47_UTC hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMaster/Schema/2016-10-06_17-17-47_UTC

	hadoop distcp hdfs://bodcdevvhdp185.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/ReducedAccountMaster/_CURRENT_VERSION hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMaster || hadoop distcp hdfs://bodcdevvhdp104.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/ReducedAccountMaster/_CURRENT_VERSION hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMaster

	hadoop distcp hdfs://bodcdevvhdp185.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/AccountMaster/Snapshot/2016-10-06_17-17-47_UTC hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMaster/Snapshot/2016-10-06_17-17-47_UTC || hadoop distcp hdfs://bodcdevvhdp104.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/AccountMaster/Snapshot/2016-10-06_17-17-47_UTC hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMaster/Snapshot/2016-10-06_17-17-47_UTC

	hadoop fs -rm -r -skipTrash /Pods/Default/Services/PropData/Sources/AccountMasterLookup

	hadoop distcp hdfs://bodcdevvhdp185.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/AccountMasterLookup/Schema/2016-10-06_21-46-16_UTC hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMasterLookup/Schema/2016-10-06_21-46-16_UTC || hadoop distcp hdfs://bodcdevvhdp104.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/AccountMasterLookup/Schema/2016-10-06_21-46-16_UTC hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMasterLookup/Schema/2016-10-06_21-46-16_UTC

	hadoop distcp hdfs://bodcdevvhdp185.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/AccountMasterLookup/_CURRENT_VERSION hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMasterLookup || hadoop distcp hdfs://bodcdevvhdp104.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/AccountMasterLookup/_CURRENT_VERSION hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMasterLookup

        hadoop distcp hdfs://bodcdevvhdp185.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/AccountMasterLookup/Snapshot/2016-10-06_21-46-16_UTC hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMasterLookup/Snapshot/2016-10-06_21-46-16_UTC || hadoop distcp hdfs://bodcdevvhdp104.dev.lattice.local:8020/Pods/QA/Services/PropData/Sources/AccountMasterLookup/Snapshot/2016-10-06_21-46-16_UTC hdfs://127.0.0.1:9000/Pods/Default/Services/PropData/Sources/AccountMasterLookup/Snapshot/2016-10-06_21-46-16_UTC
fi
