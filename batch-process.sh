#!/bin/bash
#set -e

# We'll extract, process and delete each compressed crawl data
# EXTRACTION_DIR="/mnt/ssd/census_tmp"
EXTRACTION_DIR="/tmp/census_tmp"

CENSUS_LZ4_DATA_PATH="/mnt/10tb4/census_data_lz4"
ROOT_OUT_DIR="/mnt/10tb4/census-release"

CENSUS_NORMALIZED_LZ4_DATA_PATH=${ROOT_OUT_DIR}/normalized/
mkdir -p $CENSUS_NORMALIZED_LZ4_DATA_PATH


function decompress_and_process(){
  ARCHIVE_BASE_NAME=$(basename "$1")
  CRAWL_NAME=${ARCHIVE_BASE_NAME/.tar.lz4/}
  CRAWL_DATA_PATH=$EXTRACTION_DIR/$CRAWL_NAME
  echo "Will extract $1 to $CRAWL_DATA_PATH"
  time lz4 -qdc --no-sparse $1 | tar xf - -C $EXTRACTION_DIR
  python process_crawl_data.py $CRAWL_DATA_PATH $ROOT_OUT_DIR
  python analyze_crawl.py $CRAWL_DATA_PATH $ROOT_OUT_DIR
  echo "Size before vacuuming"
  ls -hl $EXTRACTION_DIR/*201*/201*.sqlite
  time sqlite3 $EXTRACTION_DIR/*201*/*201*.sqlite 'PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF; PRAGMA temp_store = 2; VACUUM;'
  echo "Size after vacuuming"
  ls -hl $EXTRACTION_DIR/*201*/*201*.sqlite
  mkdir -p $CENSUS_NORMALIZED_LZ4_DATA_PATH/$2

  OUT_NORMALIZED_ARCHIVE=$EXTRACTION_DIR/$ARCHIVE_BASE_NAME
  pushd .
  cd $EXTRACTION_DIR
  tar c *201* | lz4 -zq - $OUT_NORMALIZED_ARCHIVE
  popd
  scp $OUT_NORMALIZED_ARCHIVE odin://mnt/10tb2/census-release-normalized/$2/
  rm $OUT_NORMALIZED_ARCHIVE
  echo "Will remove $EXTRACTION_DIR/*201*"
  rm -rf $EXTRACTION_DIR/*201*
  # !!! retain the original archive
  # rm $1
}

for crawl_archive_lz4 in $CENSUS_LZ4_DATA_PATH/$1/*.tar.lz4
  do decompress_and_process $crawl_archive_lz4 $1
done;
