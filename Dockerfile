FROM alpine:3.6

RUN apk add --no-cache ca-certificates

ENV DBSYNC_SERVICE_PORT 3000

ENV DBSYNC_DATE_TEMPLATE 2006-01-02
ENV DBSYNC_DATE_FORMAT 2006-01-02
ENV DBSYNC_TIME_TEMPLATE 2006-01-02 15:04:05
ENV DBSYNC_TIME_FORMAT 2006-01-02 15:04:05

ENV DBSYNC_UPDATE_DOCUMENTS ""
ENV DBSYNC_INSERT_DOCUMENTS ""
ENV DBSYNC_DOCUMENTS_PREFIX ""
ENV DBSYNC_DOCUMENTS_SUFFIX ""

ENV DBSYNC_INCLUDE ""
ENV DBSYNC_EXCLUDE ""

ENV DBSYNC_COLUMNS ""
ENV DBSYNC_EXTRAS ""

ENV DBSYNC_CURSOR_SPEC ""
ENV DBSYNC_ID_NAME id
ENV DBSYNC_AT_NAME at
ENV DBSYNC_START_AFTER_ID 0
ENV DBSYNC_STOP_BEFORE_ID 0
ENV DBSYNC_REVERSE false

ENV DBSYNC_UPDATE_PERIOD 300
ENV DBSYNC_INSERT_PERIOD 60

ENV DBSYNC_UPDATE_RECORDS 5000
ENV DBSYNC_INSERT_RECORDS 1000

ENV DBSYNC_DOCUMENTS_SYNC_COUNT 100

ENV DBSYNC_DOC_INDICES ""

ENV DBSYNC_FILE_DATA_DIR /var/lib/dbsync

ENV DBSYNC_FILE_JSON false
ENV DBSYNC_FILE_COMPRESSION false
ENV DBSYNC_FILE_REMOVE false
ENV DBSYNC_FILE_EXTENSION ""
ENV DBSYNC_FILE_BUCKET ""
ENV DBSYNC_FILE_TOPICS ""
ENV DBSYNC_FILE_MATCH ""
ENV DBSYNC_FILE_PATH ""
ENV DBSYNC_FILE_NAME ""
ENV DBSYNC_FILE_HEADER ""

ENV DBSYNC_SRC_DRIVER mysql

ENV DBSYNC_SRC_DB_HOST localhost
ENV DBSYNC_SRC_DB_PORT 3306
ENV DBSYNC_SRC_DB_NAME database
ENV DBSYNC_SRC_DB_USERNAME username
ENV DBSYNC_SRC_DB_PASSWORD password

ENV DBSYNC_SRC_ACCOUNT_REGION ""
ENV DBSYNC_SRC_ACCOUNT_ID id
ENV DBSYNC_SRC_ACCOUNT_KEY key
ENV DBSYNC_SRC_ACCOUNT_TOKEN ""

ENV DBSYNC_DST_DRIVER mysql

ENV DBSYNC_DST_DB_HOST localhost
ENV DBSYNC_DST_DB_PORT 3306
ENV DBSYNC_DST_DB_NAME database
ENV DBSYNC_DST_DB_USERNAME username
ENV DBSYNC_DST_DB_PASSWORD password

ENV DBSYNC_DST_ACCOUNT_REGION ""
ENV DBSYNC_DST_ACCOUNT_ID id
ENV DBSYNC_DST_ACCOUNT_KEY key
ENV DBSYNC_DST_ACCOUNT_TOKEN ""

ENV B2_LOG_LEVEL 1

EXPOSE $DBSYNC_SERVICE_PORT

COPY bin/linux/dbsync /

CMD ["/dbsync"]
