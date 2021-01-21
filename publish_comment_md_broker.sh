#!/bin/bash
PROG_PATH=$1
src=`ps aux | grep [p]ublish_comment_md.py`

if [ -n "$src" ]
then
  echo "publish_comment_md.py.py is still running"
else
  echo "running publish_comment_md.py"
  $PROG_PATH/.virtualenvs/smart-comment/bin/python3.6 $PROG_PATH/smart-comment/src/publish_comment_md.py
fi
