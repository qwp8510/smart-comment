#!/bin/bash
PROG_PATH=$1
src=`ps aux | grep [o]nline_updater.py`

if [ -n "$src" ]
then
  echo "online_updater.py is still running"
else
  echo "running online_updater.py"
  $PROG_PATH/.virtualenvs/smart-comment/bin/python3.6 $PROG_PATH/smart-comment/src/online_updater.py
fi
