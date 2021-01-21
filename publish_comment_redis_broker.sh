#!/bin/bash
PROG_PATH=$1
src=`ps aux | grep [p]ublish_comment_redis.py`

if [ -n "$src" ]
then
  echo "publish_comment_redis.py is still running"
else
  echo "running publish_comment_redis.py"
  $PROG_PATH/.virtualenvs/smart-comment/bin/python3.6 $PROG_PATH/smart-comment/src/publish_comment_redis.py
fi
