#!/bin/bash

# script to find source files for various languages on my system
#
# these files are used as training data for the code classification model.
#
# you will probably have to modify it a bit for your system.  If your code is
# generally under the same parent directory, you may be able to just change the
# value of code_root below.
#
# this script uses gshuf to randomly select files - gshuf is available on macos
# with the homebrew package `coreutils`
#
# I also didn't go out of my way to make this super reusable - to do different
# languages, just comment in/out as needed.

code_root=${HOME}/src

#lang=python
#files=$(find ${code_root} -name "*.py" -type f | grep -v "__init__.py" | grep -v "setup.py" | gshuf -n 100)

#lang=ruby
#files=$(find ${code_root} -name "*.rb" -type f | gshuf -n 100)

#lang=javascript
#files=$(find ${code_root} -name "*.js" -type f | gshuf -n 100)

#lang=java
#files=$(find ${code_root} -name "*.java" -type f | gshuf -n 100)

#lang=sql
#files=$(find ${code_root} -name "*.sql" -type f | gshuf -n 100)

#lang=text
#files=$(find ${code_root} -name "*.txt" -o -name "*.md"  -type f | grep -v "README" |  grep -v "LICENSE" | gshuf -n 100)

#lang=clike
#files=$(find ${code_root} -name "*.c" -o -name "*.cpp" -o -name "*.cc" -o -name "*.h" -type f | gshuf -n 100)

#lang=php
#files=$(find ${code_root} -name "*.php" -type f | gshuf -n 100)

#lang=golang
#files=$(find ${code_root} -name "*.go" -type f | gshuf -n 100)

#lang=kotlin
#files=$(find ${code_root} -name "*.kt" -type f | gshuf -n 100)

#lang=html
#files=$(find ${code_root} -name "*.html" -type f | gshuf -n 100)

lang=shell
files=$(find ${code_root} -name "*.sh" -type f | gshuf -n 100)

mkdir -p training_data/$lang

for f in $files
do
  cp "${f}" training_data/$lang
done