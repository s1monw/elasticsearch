BRANCH=`git rev-parse --abbrev-ref HEAD`
git checkout  master && git pull --rebase origin master
git checkout ${BRANCH} 
git rebase master

