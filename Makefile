include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: test $(PKGS)
SHELL := /bin/bash
export PATH := $(PWD)/bin:$(PATH)
PKGS = $(shell go list ./... | grep -v /vendor | grep -v /tools)
$(eval $(call golang-version-check,1.21))

export _DEPLOY_ENV=testing

test: generate $(PKGS)

$(PKGS): golang-test-all-strict-deps
	go generate $@
	$(call golang-test-all-strict,$@)

install_deps: 
	go mod vendor

bin/gomock:
	go build -o bin/gomock -mod=readonly github.com/golang/mock/gomock

bin/mockgen:
	go build -o bin/mockgen -mod=readonly github.com/golang/mock/mockgen

generate: bin/gomock bin/mockgen
	go generate ./...
	go generate ./vendor/github.com/Clever/analytics-latency-config-service/gen-go/client/interface.go
