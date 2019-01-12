#!/bin/bash

git fetch upstream develop && \
git checkout master && \
git stash && \
( git rebase upstream/develop || git stash pop ) && \
git push -f origin && \
git stash pop
