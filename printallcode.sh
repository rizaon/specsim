#!/bin/bash

cat specsim/*.py > /tmp/tmp.txt
cat main.py >> /tmp/tmp.txt
cat -n /tmp/tmp.txt > source.txt
