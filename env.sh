#!/bin/sh

export $DBSYNC_SERVICE_PORT=3000
export $DBSYNC_UPDATE_PERIOD=5
export $DBSYNC_INSERT_PERIOD=1
export $DBSYNC_UPDATE_ROWS=5000
export $DBSYNC_INSERT_ROWS=1000
export $DBSYNC_UPDATE_TABLES=''
export $DBSYNC_INSERT_TABLES=''
export $DBSYNC_SRC_DB_DRIVER='mysql'
export $DBSYNC_SRC_DB_HOST='localhost'
export $DBSYNC_SRC_DB_PORT=3306
export $DBSYNC_SRC_DB_NAME='database'
export $DBSYNC_SRC_DB_USERNAME='username'
export $DBSYNC_SRC_DB_PASSWORD='password'
export $DBSYNC_DST_DB_DRIVER='mysql'
export $DBSYNC_DST_DB_HOST='localhost'
export $DBSYNC_DST_DB_PORT=3306
export $DBSYNC_DST_DB_NAME='database'
export $DBSYNC_DST_DB_USERNAME='username'
export $DBSYNC_DST_DB_PASSWORD='password'
export $DBSYNC_DST_DB_TABLES_PREFIX=''
