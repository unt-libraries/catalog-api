#! /bin/bash
# indexfile.sh
# Test script to index files via stanford solrmarc

CONFIG_FNAME=$1
REC_FNAME=$2

# set up directories
DIST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HOMEDIR=$DIST_DIR/..
INSTANCE_BASEDIR=$HOMEDIR/instances
SITE_JAR=$DIST_DIR/StanfordSearchWorksSolrMarc.jar
CP=$SITE_JAR:$DIST_DIR:$DIST_DIR/lib
CONFIG=$DIST_DIR/$CONFIG_FNAME

java -Xmx1g -Dsolr.commit_at_end="false" -cp $CP -jar $SITE_JAR $CONFIG $REC_FNAME
