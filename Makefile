include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: test gen-bin $(PKGS)
SHELL := /bin/bash

PKGS = $(shell go list ./...)
$(eval $(call golang-version-check,1.10))

export _DEPLOY_ENV=testing

test: gen-bin $(PKGS)

gen:
	go get -u github.com/golang/mock/gomock
	go get -u github.com/golang/mock/mockgen

$(PKGS): golang-test-all-strict-deps gen
	go generate $@
	$(call golang-test-all-strict,$@)

install_deps: golang-dep-vendor-deps
	$(call golang-dep-vendor)
