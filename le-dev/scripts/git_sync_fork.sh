#!/bin/bash

git fetch upstream develop && \
git checkout master && \
git stash && \
git rebase upstream/develop && \
git push -f origin && \
git stash pop
