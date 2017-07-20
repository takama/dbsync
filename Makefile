BUILDTAGS=

# Use the 0.0.0 tag for testing, it shouldn't clobber any release builds
APP?=dbsync
USERSPACE?=takama
RELEASE?=0.2.4
PROJECT?=github.com/${USERSPACE}/${APP}
HELM_REPO?=https://${USERSPACE}.github.io/${APP}
GOOS?=linux
REGISTRY?=docker.io

DBSYNC_SERVICE_PORT?=3000

DBSYNC_UPDATE_DOCUMENTS?=""
DBSYNC_INSERT_DOCUMENTS?=""
DBSYNC_DOCUMENTS_PREFIX?=""
DBSYNC_DOCUMENTS_SUFFIX?=""

DBSYNC_START_AFTER_ID?=0

DBSYNC_UPDATE_PERIOD?=300
DBSYNC_INSERT_PERIOD?=60

DBSYNC_UPDATE_RECORDS?=5000
DBSYNC_INSERT_RECORDS?=1000

DBSYNC_DOCUMENTS_SYNC_COUNT?=1000

DBSYNC_FILE_DATA_DIR?=/var/lib/dbsync

DBSYNC_SRC_DRIVER?=mysql

DBSYNC_SRC_DB_HOST?=localhost
DBSYNC_SRC_DB_PORT?=3306
DBSYNC_SRC_DB_NAME?=database
DBSYNC_SRC_DB_USERNAME?=username
DBSYNC_SRC_DB_PASSWORD?=password

DBSYNC_SRC_ACCOUNT_REGION?=""
DBSYNC_SRC_ACCOUNT_ID?=id
DBSYNC_SRC_ACCOUNT_KEY?=key
DBSYNC_SRC_ACCOUNT_TOKEN?=""

DBSYNC_SRC_FILE_JSON?=false
DBSYNC_SRC_FILE_COMPRESSION?=false
DBSYNC_SRC_FILE_REMOVE?=false
DBSYNC_DST_FILE_EXTENSION?=""
DBSYNC_SRC_FILE_BUCKET?=""
DBSYNC_SRC_FILE_ID?=id
DBSYNC_SRC_FILE_TOPICS?=""
DBSYNC_SRC_FILE_MATCH?=""
DBSYNC_SRC_FILE_EXCLUDE?=""
DBSYNC_SRC_FILE_SPEC?=""
DBSYNC_SRC_FILE_PATH?=""
DBSYNC_SRC_FILE_NAME?=""
DBSYNC_SRC_FILE_HEADER?=""
DBSYNC_SRC_FILE_COLUMNS?=""

DBSYNC_DST_DRIVER?=mysql

DBSYNC_DST_DB_HOST?=localhost
DBSYNC_DST_DB_PORT?=3306
DBSYNC_DST_DB_NAME?=database
DBSYNC_DST_DB_USERNAME?=username
DBSYNC_DST_DB_PASSWORD?=password

DBSYNC_DST_ACCOUNT_REGION?=""
DBSYNC_DST_ACCOUNT_ID?=id
DBSYNC_DST_ACCOUNT_KEY?=key
DBSYNC_DST_ACCOUNT_TOKEN?=""

DBSYNC_DST_FILE_JSON?=false
DBSYNC_DST_FILE_COMPRESSION?=false
DBSYNC_DST_FILE_EXTENSION?=.copy
DBSYNC_DST_FILE_BUCKET?=""
DBSYNC_DST_FILE_ID?="id"
DBSYNC_DST_FILE_TOPICS?=""
DBSYNC_DST_FILE_MATCH?=""
DBSYNC_DST_FILE_EXCLUDE?=""
DBSYNC_DST_FILE_SPEC?=""
DBSYNC_DST_FILE_PATH?=""
DBSYNC_DST_FILE_NAME?=""
DBSYNC_DST_FILE_HEADER?=""
DBSYNC_DST_FILE_COLUMNS?=""

NAMESPACE?=${USERSPACE}
PREFIX?=${REGISTRY}/${NAMESPACE}/${APP}
CONTAINER_NAME?=${APP}-${NAMESPACE}

ifeq ($(NAMESPACE), default)
	PREFIX=${REGISTRY}/${APP}
	CONTAINER_NAME=${APP}
endif

REPO_INFO=$(shell git config --get remote.origin.url)

ifndef COMMIT
	COMMIT := git-$(shell git rev-parse --short HEAD)
endif

.PHONY: all
all: push

.PHONY: vendor
vendor: clean bootstrap
	glide install --strip-vendor

.PHONY: build
build: vendor
	CGO_ENABLED=0 GOOS=${GOOS} go build -a -installsuffix cgo \
		-ldflags "-s -w -X ${PROJECT}/pkg/version.RELEASE=${RELEASE} -X ${PROJECT}/pkg/version.COMMIT=${COMMIT} -X ${PROJECT}/pkg/version.REPO=${REPO_INFO}" \
		-o ./bin/${GOOS}/${APP} ${PROJECT}/cmd

.PHONY: container
container: build
	docker build --pull -t $(PREFIX):$(RELEASE) .

.PHONY: push
push: container
	docker push $(PREFIX):$(RELEASE)

.PHONY: run
run: stop container
	docker run --name ${CONTAINER_NAME} -p ${DBSYNC_SERVICE_PORT}:${DBSYNC_SERVICE_PORT} \
		-e "DBSYNC_SERVICE_PORT=${DBSYNC_SERVICE_PORT}" \
		-e "DBSYNC_UPDATE_DOCUMENTS=${DBSYNC_UPDATE_DOCUMENTS}" \
		-e "DBSYNC_INSERT_DOCUMENTS=${DBSYNC_INSERT_DOCUMENTS}" \
		-e "DBSYNC_DOCUMENTS_PREFIX=${DBSYNC_DOCUMENTS_PREFIX}" \
		-e "DBSYNC_DOCUMENTS_SUFFIX=${DBSYNC_DOCUMENTS_SUFFIX}" \
		-e "DBSYNC_START_AFTER_ID=${DBSYNC_START_AFTER_ID}" \
		-e "DBSYNC_UPDATE_PERIOD=${DBSYNC_UPDATE_PERIOD}" \
		-e "DBSYNC_INSERT_PERIOD=${DBSYNC_INSERT_PERIOD}" \
		-e "DBSYNC_UPDATE_RECORDS=${DBSYNC_UPDATE_RECORDS}" \
		-e "DBSYNC_INSERT_RECORDS=${DBSYNC_INSERT_RECORDS}" \
		-e "DBSYNC_DOCUMENTS_SYNC_COUNT=${DBSYNC_DOCUMENTS_SYNC_COUNT}" \
		-e "DBSYNC_FILE_DATA_DIR=${DBSYNC_FILE_DATA_DIR}" \
		-e "DBSYNC_SRC_DRIVER=${DBSYNC_SRC_DRIVER}" \
		-e "DBSYNC_SRC_DB_HOST=${DBSYNC_SRC_DB_HOST}" \
		-e "DBSYNC_SRC_DB_PORT=${DBSYNC_SRC_DB_PORT}" \
		-e "DBSYNC_SRC_DB_NAME=${DBSYNC_SRC_DB_NAME}" \
		-e "DBSYNC_SRC_DB_USERNAME=${DBSYNC_SRC_DB_USERNAME}" \
		-e "DBSYNC_SRC_DB_PASSWORD=${DBSYNC_SRC_DB_PASSWORD}" \
		-e "DBSYNC_SRC_ACCOUNT_REGION=${DBSYNC_SRC_ACCOUNT_REGION}" \
		-e "DBSYNC_SRC_ACCOUNT_ID=${DBSYNC_SRC_ACCOUNT_ID}" \
		-e "DBSYNC_SRC_ACCOUNT_KEY=${DBSYNC_SRC_ACCOUNT_KEY}" \
		-e "DBSYNC_SRC_ACCOUNT_TOKEN=${DBSYNC_SRC_ACCOUNT_TOKEN}" \
		-e "DBSYNC_SRC_FILE_JSON=${DBSYNC_SRC_FILE_JSON}" \
		-e "DBSYNC_SRC_FILE_COMPRESSION=${DBSYNC_SRC_FILE_COMPRESSION}" \
		-e "DBSYNC_SRC_FILE_REMOVE=${DBSYNC_SRC_FILE_REMOVE}" \
		-e "DBSYNC_SRC_FILE_EXTENSION=${DBSYNC_SRC_FILE_EXTENSION}" \
		-e "DBSYNC_SRC_FILE_BUCKET=${DBSYNC_SRC_FILE_BUCKET}" \
		-e "DBSYNC_SRC_FILE_ID=${DBSYNC_SRC_FILE_ID}" \
		-e "DBSYNC_SRC_FILE_TOPICS=${DBSYNC_SRC_FILE_TOPICS}" \
		-e "DBSYNC_SRC_FILE_MATCH=${DBSYNC_SRC_FILE_MATCH}" \
		-e "DBSYNC_SRC_FILE_EXCLUDE=${DBSYNC_SRC_FILE_EXCLUDE}" \
		-e "DBSYNC_SRC_FILE_SPEC=${DBSYNC_SRC_FILE_SPEC}" \
		-e "DBSYNC_SRC_FILE_PATH=${DBSYNC_SRC_FILE_PATH}" \
		-e "DBSYNC_SRC_FILE_NAME=${DBSYNC_SRC_FILE_NAME}" \
		-e "DBSYNC_SRC_FILE_HEADER=${DBSYNC_SRC_FILE_HEADER}" \
		-e "DBSYNC_SRC_FILE_COLUMNS=${DBSYNC_SRC_FILE_COLUMNS}" \
		-e "DBSYNC_DST_DRIVER=${DBSYNC_DST_DRIVER}" \
		-e "DBSYNC_DST_DB_HOST=${DBSYNC_DST_DB_HOST}" \
		-e "DBSYNC_DST_DB_PORT=${DBSYNC_DST_DB_PORT}" \
		-e "DBSYNC_DST_DB_NAME=${DBSYNC_DST_DB_NAME}" \
		-e "DBSYNC_DST_DB_USERNAME=${DBSYNC_DST_DB_USERNAME}" \
		-e "DBSYNC_DST_DB_PASSWORD=${DBSYNC_DST_DB_PASSWORD}" \
		-e "DBSYNC_DST_ACCOUNT_REGION=${DBSYNC_DST_ACCOUNT_REGION}" \
		-e "DBSYNC_DST_ACCOUNT_ID=${DBSYNC_DST_ACCOUNT_ID}" \
		-e "DBSYNC_DST_ACCOUNT_KEY=${DBSYNC_DST_ACCOUNT_KEY}" \
		-e "DBSYNC_DST_ACCOUNT_TOKEN=${DBSYNC_DST_ACCOUNT_TOKEN}" \
		-e "DBSYNC_DST_FILE_JSON=${DBSYNC_DST_FILE_JSON}" \
		-e "DBSYNC_DST_FILE_COMPRESSION=${DBSYNC_DST_FILE_COMPRESSION}" \
		-e "DBSYNC_DST_FILE_EXTENSION=${DBSYNC_DST_FILE_EXTENSION}" \
		-e "DBSYNC_DST_FILE_BUCKET=${DBSYNC_DST_FILE_BUCKET}" \
		-e "DBSYNC_DST_FILE_ID=${DBSYNC_DST_FILE_ID}" \
		-e "DBSYNC_DST_FILE_TOPICS=${DBSYNC_DST_FILE_TOPICS}" \
		-e "DBSYNC_DST_FILE_MATCH=${DBSYNC_DST_FILE_MATCH}" \
		-e "DBSYNC_DST_FILE_EXCLUDE=${DBSYNC_DST_FILE_EXCLUDE}" \
		-e "DBSYNC_DST_FILE_SPEC=${DBSYNC_DST_FILE_SPEC}" \
		-e "DBSYNC_DST_FILE_PATH=${DBSYNC_DST_FILE_PATH}" \
		-e "DBSYNC_DST_FILE_NAME=${DBSYNC_DST_FILE_NAME}" \
		-e "DBSYNC_DST_FILE_HEADER=${DBSYNC_DST_FILE_HEADER}" \
		-e "DBSYNC_DST_FILE_COLUMNS=${DBSYNC_DST_FILE_COLUMNS}" \
		-v ${DBSYNC_FILE_DATA_DIR}:${DBSYNC_FILE_DATA_DIR} \
		-d $(PREFIX):$(RELEASE)
	sleep 1
	docker logs ${CONTAINER_NAME}

HAS_RUNNED := $(shell docker ps | grep ${CONTAINER_NAME})
HAS_EXITED := $(shell docker ps -a | grep ${CONTAINER_NAME})

.PHONY: logs
logs:
	docker logs ${CONTAINER_NAME}

.PHONY: stop
stop:
ifdef HAS_RUNNED
	@docker stop ${CONTAINER_NAME}
endif
ifdef HAS_EXITED
	@docker rm ${CONTAINER_NAME}
endif

.PHONY: deploy
deploy: push
	helm repo add ${USERSPACE} ${HELM_REPO}
	helm repo up
	helm upgrade ${CONTAINER_NAME} ${USERSPACE}/${APP} --namespace ${NAMESPACE} --set image.tag=${RELEASE} -i --wait

GO_LIST_FILES=$(shell go list ${PROJECT}/... | grep -v vendor)

.PHONY: fmt
fmt:
	@echo "+ $@"
	@go list -f '{{if len .TestGoFiles}}"gofmt -s -l {{.Dir}}"{{end}}' ${GO_LIST_FILES} | xargs -L 1 sh -c

.PHONY: lint
lint:
	@echo "+ $@"
	@go list -f '{{if len .TestGoFiles}}"golint -min_confidence=0.85 {{.Dir}}/..."{{end}}' ${GO_LIST_FILES} | xargs -L 1 sh -c

.PHONY: vet
vet:
	@echo "+ $@"
	@go vet ${GO_LIST_FILES}

.PHONY: test
test: vendor fmt lint vet
	@echo "+ $@"
	@go test -v -race -tags "$(BUILDTAGS) cgo" ${GO_LIST_FILES}

.PHONY: cover
cover:
	@echo "+ $@"
	@go list -f '{{if len .TestGoFiles}}"go test -coverprofile={{.Dir}}/.coverprofile {{.ImportPath}}"{{end}}' ${GO_LIST_FILES} | xargs -L 1 sh -c

.PHONY: clean
clean:
	rm -f ./bin/${GOOS}/${APP}

HAS_GLIDE := $(shell command -v glide;)
HAS_LINT := $(shell command -v golint;)

.PHONY: bootstrap
bootstrap:
ifndef HAS_GLIDE
	go get -u github.com/Masterminds/glide
endif
ifndef HAS_LINT
	go get -u github.com/golang/lint/golint
endif
