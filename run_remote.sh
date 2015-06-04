#!/bin/bash

HOST=10.0.1.2
USER=simon
BRANCH=`git rev-parse --abbrev-ref HEAD`
REMOTE=simon
FORK_PATH=projects/elasticsearch
git push ${REMOTE} ${BRANCH}

CMD="cd ${FORK_PATH} && git fetch ${REMOTE} && git checkout $BRANCH && git reset --hard ${REMOTE}/$BRANCH && mvn clean test -Dtests.jvms=8 -Dtests.rest=false"
echo $CMD
ssh -A ${USER}@${HOST} ${CMD}
