include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: test $(PKGS)
SHELL := /bin/bash

PKGS = $(shell go list ./...)
$(eval $(call golang-version-check,1.10))

export _DEPLOY_ENV=testing

test: $(PKGS)

$(PKGS): golang-test-all-strict-deps
	go generate $@
	$(call golang-test-all-strict,$@)

install_deps: golang-dep-vendor-deps
	go get -u github.com/golang/mock/gomock
	go get -u github.com/golang/mock/mockgen
	$(call golang-dep-vendor)
