#!/bin/bash
# Arguments: 3 - /abs/path/to/data mnemonic hadoop_username
# eg. run.sh /somenode/scratch/something/L241833_SAP002_B000_P002/products/ L241833_SAP002_B000_P002 emilioe
#
# Assumptions:
# /abs/path/to/data/$mnemonic_HighRes.fil exists, .dats and .logs here as well
# username.keytab is present in same location as this script
#
# HDFS view:
# /data/private/setinl/$mnemonic/$mnemonic_HighRes.fil.gz will be created
# /data/private/setinl/$mnemonic/$mnemonic_logsdats.tar.gz will be created
# /data/private/setinl/$mnemonic/parquet will be created

dataPath=$1
mnemonic=$2
username=$3

if [ -z ${HATHI_PATH} ]; then
    HATHI_PATH=/cvmfs/softdrive.nl/"$username"/hathi-client
fi
echo $HATHI_PATH

eval $("$HATHI_PATH"/bin/env.sh)
kinit -k -t "$username".keytab "$username"@CUA.SURFSARA.NL

hdfs dfs -get /data/private/seti/seticombine .
chmod -R +x seticombine

hdfs dfs -mkdir /data/private/seti/output/"$mnemonic"
seticombine/bin/seticombine $mnemonic $dataPath /data/private/seti/output

if [[ "$?" != 0 ]]; then
	echo "Problem running seticombine..."
	exit 1
fi

hdfs dfs -ls /data/private/seti/output/"$mnemonic"/parquet && hdfs dfs -ls /data/private/seti/output/"$mnemonic"/"$mnemonic"_HighRes.fil.gz && hdfs dfs -ls /data/private/seti/output/"$mnemonic"/"$mnemonic"_datslogs.tar.gz

if [[ "$?" != 0 ]]; then
	echo "Seticombine ended correctly but files do not exist on HDFS..."
	exit 1
fi
