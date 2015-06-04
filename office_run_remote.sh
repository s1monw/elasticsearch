#!/bin/bash

HOST=monster
USER=simon
BRANCH=`git rev-parse --abbrev-ref HEAD`
REMOTE=simon
FORK_PATH="/media/benchmark/elasticsearch"
git push ${REMOTE} ${BRANCH}

CMD="cd ${FORK_PATH} && git fetch ${REMOTE} && git checkout $BRANCH && git reset --hard ${REMOTE}/$BRANCH && mvn3 -Pdev clean test -Dtests.jvms=8 -Des.logger.level=DEBUG -Dtests.security.manager=true -Dtests.slow=true -Des.node.mode=local"
echo $CMD

ssh -A ${USER}@${HOST} ${CMD}
