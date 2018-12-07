#!/usr/bin/env bash

# https://gist.github.com/karenyyng/f19ff75c60f18b4b8149
git config --global merge.tool vimdiff
git config --global merge.conflictstyle diff3

# global alias
git config --global alias.ls "log --graph --abbrev-commit --decorate --format=format:'%C(bold blue)%h%C(reset) - %C(bold green)(%ar)%C(reset) %C(white)%s%C(reset) %C(dim white)- %an%C(reset)%C(bold yellow)%d%C(reset)' --all"
