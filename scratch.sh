#!/bin/bash
echo start


dwhTable="sf.asset"
sfObject="Asset"
toS3Job="sfAssetIncrementalLoadToS3"
toRedshiftJob="sfAssetIncrementalLoadToRedshift"
triggerName="startAssets"
tableName="assetincrementalload"
databaseName="sfdb"
crawlerName="sfAssetIncrementalLoadToS3"

doInitialLoad="--DO_INITIAL_LOAD=y"


folder="$(tr [A-Z] [a-z] <<< "$sfObject")"
outPath="s3://sf-stg/${folder}/${sfObject}IncrementalLoad/"
echo "s3 folder = $outPath"

echo "stopping workflow"
aws glue stop-trigger --name $triggerName

echo "clearing s3 folder"
aws s3 rm $outPath --recursive

echo "deleting table from data catalog"
aws glue delete-table --database-name $databaseName --name $tableName


echo "you must truncate table $dwhTable and add new fields"
echo "Press any key to continue"
while [ true ] ; do
read -t 3 -n 1
if [ $? = 0 ] ; then
break ;
else
echo "truncate table $dwhTable, add new fields and press any key"
fi
done

echo "start loading data to S3"
toS3JobId="$(aws glue start-job-run --job-name "$toS3Job" --arguments=$doInitialLoad)"
echo "s3JobId = $toS3JobId"
shortJobId=${toS3JobId:10}
echo "short s3JobId = $shortJobId"


getJobInfo () {
  local jobInfo="$(aws glue get-job-run --job-name "$toS3Job" --run-id "$shortJobId")"
  echo "$jobInfo"
}

jobInfo="$(getJobInfo)"
echo $jobInfo

while true; do
  if echo "$(getJobInfo)" | grep -q "FAILED"; then
    echo "loading to S3 is failed";
    echo "$(getJobInfo)"
    exit;
  elif echo "$(getJobInfo)" | grep -q "RUNNING"; then
    echo "loading to S3 is running yet";
    sleep 30;
  else
    break;
  fi
done
echo "loading to S3 is done!"

echo "start crawler"
aws glue start-crawler --name $crawlerName


getCrawlerInfo () {
  local crawlerInfo="$(aws glue get-crawler --name "$crawlerName")"
  echo "$crawlerInfo"
}

crawlerInfo="$(getCrawlerInfo)"
echo $crawlerInfo

while true; do
  if echo "$(getCrawlerInfo)" | grep -x "State: FAILED"; then
    echo "crawling is failed";
    echo "$(getCrawlerInfo)"
    exit;
  elif echo "$(getCrawlerInfo)" | grep -q "RUNNING"; then
    echo "crawling is running yet";
    sleep 30;
  elif echo "$(getCrawlerInfo)" | grep -q "STOPPING"; then
    echo "crawling is running yet";
    sleep 30;
  elif echo "$(getCrawlerInfo)" | grep -q "STARTING"; then
    echo "crawling is running yet";
    sleep 30;
  else
    break;
  fi
done
echo "crawling is done!"


echo "you must add mappings to ToRedshiftJob"
echo "Press any key to continue"
while [ true ] ; do
read -t 3 -n 1
if [ $? = 0 ] ; then
break ;
else
echo "add mappings to ToRedshiftJob and press any key "
fi
done


echo "start loading data to Redshift"
toRedshiftJobId="$(aws glue start-job-run --job-name "$toRedshiftJob" --arguments=$doInitialLoad)"

echo "RedshiftJobId = $toRedshiftJobId"
shortJobId=${toRedshiftJobId:10}
echo "short RedshiftJobId = $shortJobId"

getRedshiftJobInfo () {
  local jobInfo="$(aws glue get-job-run --job-name "$toRedshiftJob" --run-id "$shortJobId")"
  echo "$jobInfo"
}

jobInfo="$(getRedshiftJobInfo)"
echo $jobInfo

while true; do
  if echo "$(getRedshiftJobInfo)" | grep -q "FAILED"; then
    echo "loading to Redshift is failed";
    echo "$(getJobInfo)"
    exit;
  elif echo "$(getRedshiftJobInfo)" | grep -q "RUNNING"; then
    echo "loading to Redshift is running yet";
    sleep 30;
  else
    break;
  fi
done
echo "loading to Redshift is done!"

echo "start workflow"
aws glue start-trigger --name $triggerName