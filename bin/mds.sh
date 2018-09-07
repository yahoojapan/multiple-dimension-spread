#!/bin/sh

basedir=`dirname $0`
libdir="$basedir/../jars"
dependency_libdir="$libdir/lib"

function print_help(){
  echo "setup  setup mds lib dir." >&2
  $JAVA_CMD -cp ".:$dependency_libdir/*:$libdir/*" jp.co.yahoo.dataplatform.mds.tools.MDSTool help
}

if [ ! -e "$libdir" ];then
  echo "$libdir is not found." >&2
  echo "Please setup \"$basedir/setup.sh \" " >&2
  exit 255
fi

if [ -z "$JAVA_HOME" ]; then
  echo "JAVA_HOME is not set." >&2
  exit 255
fi

JAVA_CMD="$JAVA_HOME/bin/java"

if [ ! -e $JAVA_CMD ];then
  echo "$JAVA_CMD is not found." >&2
  exit 255
fi

if [ $# -le 0 ];then
  print_help
  exit 255
fi

if [ "setup" = "$1" ];then
  /bin/sh $basedir/setup.sh
  exit $?
fi

if [ "help" = "$1" ];then
  print_help
  exit 0
fi

$JAVA_CMD -cp ".:$dependency_libdir/*:$libdir/*" jp.co.yahoo.dataplatform.mds.tools.MDSTool $@
