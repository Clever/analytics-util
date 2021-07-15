include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: test $(PKGS)
SHELL := /bin/bash

PKGS = $(shell go list ./... | grep -v /vendor | grep -v /tools)
$(eval $(call golang-version-check,1.13))

export _DEPLOY_ENV=testing

test: generate $(PKGS)

$(PKGS): golang-test-all-strict-deps
	go generate $@
	$(call golang-test-all-strict,$@)

install_deps:
	go mod vendor
	go get -u github.com/golang/mock/gomock
	go get -u github.com/golang/mock/mockgen

generate:
	go generate ./...
	go generate ./vendor/github.com/Clever/analytics-latency-config-service/gen-go/client/interface.go
