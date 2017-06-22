FROM scratch

ENV DBSYNC_SERVICE_PORT 3000
ENV DBSYNC_UPDATE_PERIOD 5
ENV DBSYNC_INSERT_PERIOD 1
ENV DBSYNC_UPDATE_ROWS 5000
ENV DBSYNC_INSERT_ROWS 1000
ENV DBSYNC_START_AFTER_ID 0
ENV DBSYNC_UPDATE_TABLES ''
ENV DBSYNC_INSERT_TABLES ''
ENV DBSYNC_SRC_DB_DRIVER 'mysql'
ENV DBSYNC_SRC_DB_HOST 'localhost'
ENV DBSYNC_SRC_DB_PORT 3306
ENV DBSYNC_SRC_DB_NAME 'database'
ENV DBSYNC_SRC_DB_USERNAME 'username'
ENV DBSYNC_SRC_DB_PASSWORD 'password'
ENV DBSYNC_DST_DB_DRIVER 'mysql'
ENV DBSYNC_DST_DB_HOST 'localhost'
ENV DBSYNC_DST_DB_PORT 3306
ENV DBSYNC_DST_DB_NAME 'database'
ENV DBSYNC_DST_DB_USERNAME 'username'
ENV DBSYNC_DST_DB_PASSWORD 'password'
ENV DBSYNC_DST_DB_TABLES_PREFIX ''
ENV DBSYNC_DST_DB_TABLES_POSTFIX ''

EXPOSE $DBSYNC_SERVICE_PORT

COPY bin/linux/dbsync /

CMD ["/dbsync"]
