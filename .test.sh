#!/bin/sh

for d in $(go list ./...); do
    if [ $1 == "race" ]; then
        echo "race"
        go test -race || exit 2
    else 
        set -e;
        echo "" > coverage.txt;

        go test -v -coverprofile=profile.out -covermode=atomic $d || exit 1
        if [ -f profile.out ]; then
            cat profile.out >> coverage.txt;
            rm profile.out;
        fi
    fi
done