#!/bin/bash
set -e

CENSUS_DATA_PATH="/mnt/10tb3/census_data"
OUTDIR="/mnt/10tb4"

function convert_bz2_to_lz4(){
 ARCHIVE_BASE_NAME=$(basename "$1")
 ARCHIVE_OUT_NAME=${ARCHIVE_BASE_NAME/bz2/lz4}
 OUT_PATH=$OUTDIR/$ARCHIVE_OUT_NAME
 echo "Will convert $1 to $OUT_PATH"
 pbzip2 -cd $1 |  lz4 -z - $OUT_PATH
}

for crawl_archive in $CENSUS_DATA_PATH/*.bz2
  do convert_bz2_to_lz4 $crawl_archive
done;
