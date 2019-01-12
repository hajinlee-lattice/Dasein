#!/bin/bash

git fetch upstream && \
git checkout master && \
git rebase upstream/develop && \
git push -f origin
