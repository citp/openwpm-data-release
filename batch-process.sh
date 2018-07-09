#!/bin/bash
set -e

CENSUS_LZ4_DATA_PATH="/mnt/10tb4/census_data_lz4"

# We'll extract, process and delete each compressed crawl data
EXTRACTION_DIR="/mnt/ssd/census_tmp"

function decompress_and_process(){
  ARCHIVE_BASE_NAME=$(basename "$1")
  CRAWL_NAME=${ARCHIVE_BASE_NAME/.tar.lz4/}
  CRAWL_DATA_PATH=$EXTRACTION_DIR/$CRAWL_NAME
  echo "Will extract $1 to $CRAWL_DATA_PATH"
  time lz4 -dc --no-sparse $1 | tar xf - -C $EXTRACTION_DIR
  time python process_crawl_data.py $CRAWL_DATA_PATH
  # ls -l $EXTRACTION_DIR/201*/201*.sqlite
  # echo "Will vacuum the database"
  # time sqlite3 $EXTRACTION_DIR/201*/201*.sqlite 'VACUUM;'
  # ls -l $EXTRACTION_DIR/201*/201*.sqlite
  echo "Will remove $EXTRACTION_DIR/201*"
  rm -rf $EXTRACTION_DIR/201*
}

for crawl_archive_lz4 in $CENSUS_LZ4_DATA_PATH/*.tar.lz4
  do decompress_and_process $crawl_archive_lz4
done;
