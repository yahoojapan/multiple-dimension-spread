#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0); pwd)
mds_deliver_tool='mds_deliver'
hdfs_mds_lib='/mds_lib'
external_jar_files=(
  multiple-dimension-spread-tools
)

function show_usage() {
cat << EOS
$0 [-i mds_source] [-l hdfs_mds_lib]
gathering mds related jar files to setup environment.

Option: mds_source
not defined:   get the release version from maven repository.
versions file: get the version described in the versions file from maven repository.
               example versions file is bin/$mds_deliver_tool/versions.sh.template.
mds directory: get the jar files from mds directory located following repositories,
               - dataplatform-config
               - dataplatform-schema-lib
               - multiple-dimension-spread
               and create jar using "mvn package" at each directories.

Option: hdfs_mds_lib
  define mds libraly location in HDFS to create add_jar.hql .
  default value is "$hdfs_mds_lib" .
EOS
  exit 0
}


function create_libdir() {
  libdir="$SCRIPT_DIR/../jars"
  if [ -d $libdir ]; then rm -r $libdir; fi
  mkdir -p $libdir
  mkdir $libdir/mds
}


function get_external_jars() {
  local fn
  local not_found_files=()
  for fn in ${external_jar_files[@]}
  do
    local src_fn=$(find $SCRIPT_DIR/../src -name $fn-*.jar | sort | grep -v sources | grep -v javadoc)
    if [ ! -z "$src_fn" ]
    then cp $src_fn $libdir/$fn.jar
    else not_found_files+=($fn)
    fi
  done

  if [ ${#not_found_files[@]} != 0 ]; then
cat << EOS
Please execute 'mvn package' to create external jar files for command tools.
Following external jar files are not found in this dir.
EOS
    for fn in ${not_found_files[@]}
    do
      echo $fn
    done
    exit -1
  fi
}

function get_mds_jars() {
  local src=$1
  local hdfs_mds_lib=$2
  local local_jars=local

  local src_dn
  local mds_tool=$SCRIPT_DIR/$mds_deliver_tool
  local mds_jars=$mds_tool/jars/$local_jars
  $mds_tool/get_jar.sh get -i $src -l $hdfs_mds_lib -o $local_jars
  for src_dn in `find $mds_jars -type d -name latest`
  do
    local dist_dn=$(echo $src_dn | cut -b $((${#mds_jars}+2))- | cut -d/ -f1)
    mv $src_dn $libdir/mds/$dist_dn
  done
  cat $mds_jars/add_jar.hql | sed 's/latest\///' > $libdir/mds/add_jar.hql
  rm -r $mds_jars
}

function get_lib_jars() {
  local dependency_libdir="$libdir/lib"
  mkdir $dependency_libdir
  cp $SCRIPT_DIR/../etc/dependency_pom.xml.template $dependency_libdir/pom.xml
  mvn dependency:copy-dependencies -DoutputDirectory=. -f $dependency_libdir/pom.xml
  rm $dependency_libdir/pom.xml
}

function get_jars() {
  create_libdir
  get_external_jars
  get_mds_jars $*
  get_lib_jars
}


mds_source=$SCRIPT_DIR/mds_deliver/version.sh
while getopts i:l:h OPT
do
  case $OPT in
    i)  mds_source="$OPTARG" ;;
    l)  hdfs_mds_lib="$OPTARG" ;;
    h)  show_usage ;;
    \?) show_usage ;;
  esac
done
shift $((OPTIND - 1))

get_jars $mds_source $hdfs_mds_lib

