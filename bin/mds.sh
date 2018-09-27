#!/usr/bin/env bash
basedir=$(cd $(dirname $0); pwd)

libdir=$(cd "$basedir/../jars"; pwd)
if [ ! -d "$libdir" ]; then
  echo "$libdir is not found." >&2
  echo "Please setup \"$basedir/setup.sh \" " >&2
  exit 255
fi

function java_exec() {
  local JAVA_CMD="$JAVA_HOME/bin/java"
  if [ ! -e $JAVA_CMD ]; then JAVA_CMD=java; fi

  local dn
  local lib_paths=($libdir/lib $libdir)
  lib_paths+=($(find $libdir/mds -type d -d -2 | sed -e :loop -e 'N; $!b loop' -e 's/\n/ /g'))
  local class_paths='.'
  for dn in ${lib_paths[@]}
  do
    class_paths="$class_paths:$dn/*"
  done
  $JAVA_CMD -cp "$class_paths" jp.co.yahoo.dataplatform.mds.tools.MDSTool $*
}

function show_usage() {
  echo "setup  setup mds lib dir." >&2
  java_exec help
  exit $1
}

case "$1" in
  ""      ) show_usage 255 ;;
  "setup" ) shift; $basedir/setup.sh $*; exit $? ;;
  "help"  ) show_usage 0 ;;
  "-h"    ) show_usage 0 ;;
  * ) java_exec $*
esac

