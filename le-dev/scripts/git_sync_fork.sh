#!/bin/bash

git fetch upstream develop && \
git checkout master && \
git stash && \
( git rebase upstream/develop || ( git stash pop; exit -1 ) ) && \
git push -f origin && \
git stash pop
