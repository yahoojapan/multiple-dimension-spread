#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0); pwd)
com=$1
shift

function show_usage() {
cat << EOS
$0 command
command:
list: get version list from maven repository

get:  get [-i source] [-o target_dir] [-l hdfs_lib]
      get jar from maven repository

      source: that is where to get jer files from
              case file:   read as version file
              case dir:    gather jar files from local directory
              not defined: use versions.sh as version file

      version file is for setting of mds, db_schema and dp_config
      if one or some versions are not exists, release version are gotten.

      target_dir: default is YYYYMMDD in jars/
      version is described by file name as ${module}/latest/version/${version}

      hdfs_lib: target hdfs directory to storage mds lib.

to:   to target_server [target_dir]
      upload jars/target_dir to your home directory in the target_server
      target_dir: default is YYYYMMDD in jars/

EOS
  exit 0
}

tmp_dir=$SCRIPT_DIR/jars/tmp
mvn_url=https://repo1.maven.org/maven2/jp/co/yahoo/dataplatform
module_settings=(
  'mds mds/multiple-dimension-spread'
  'dp_schema schema/schema'
  'dp_config config/dataplatform-common-config'
)


function get_version_list() {
  for ((i=0; ${#module_settings[*]}>$i; i++)); do
    local tmp=(${module_settings[$i]})
    local category=${tmp[0]}
    local module=${tmp[1]}
    echo $category

    curl -s $mvn_url/$module/maven-metadata.xml \
      | grep "<version>" \
      | sed -E 's/ *<version> *//g' \
      | sed -E 's/ *<\/version> *//g'
    echo
  done
  exit 0
}


function get_release_version() {
  local category=$1
  local version=$2
  if [ ! -z $version ]; then
    release_version=$version
    return
  fi

  for ((i=0; ${#module_settings[*]}>$i; i++)); do
    local tmp=(${module_settings[$i]})
    local module=${tmp[1]}
    if [ ${tmp[0]} == $category ]; then
      release_version=$(curl -s $mvn_url/$module/maven-metadata.xml \
        | grep "<release>" \
        | sed -E 's/ *<release> *//g' \
        | sed -E 's/ *<\/release> *//g')
      return
    fi
  done
}


function copy_jars() {
  local category=$1
  local src_dir=$2
  local target_dir=$3
  local version_ext=$4
  local version

  for fn in ${jars[@]}; do
    src=$(find $src_dir -name $fn-*jar | sort | tail -n 1)
    version=$(echo $src | sed 's/.*'$fn'-\(.*\)\.jar/\1/')$version_ext
    for v in $version latest; do
      mkdir -p $target_dir/$category/$v
      cp $src $target_dir/$category/$v/${fn}.jar
    done
  done

  local version_dir=$target_dir/$category/latest/version
  if [ -d $version_dir ]; then rm -r $version_dir; fi
  mkdir -p $version_dir
  touch $version_dir/$version
}


function gathering_jars() {
  local src_dir=$1
  local target_dir=$2
  local version_ext=$3
  if [ -z $version_ext ]; then version_ext=''; fi

  jars=(
    multiple-dimension-spread-arrow
    multiple-dimension-spread-common
    multiple-dimension-spread-hadoop
    multiple-dimension-spread-schema
    multiple-dimension-spread-hive
  )
  copy_jars mds $src_dir $target_dir $version_ext

  jars=(
    schema-common
    schema-hive
    schema-jackson
  )
  copy_jars dp_schema $src_dir $target_dir $version_ext

  jars=(
    dataplatform-common-config
  )
  copy_jars dp_config $src_dir $target_dir $version_ext
}


function get_jars_from_mvn() {
  local target_dir=$1
  get_release_version mds $mds; mds=$release_version
  get_release_version dp_schema $dp_schema; dp_schema=$release_version
  get_release_version dp_config $dp_config; dp_config=$release_version

  sed "s/\$mds_version/$mds/g" $SCRIPT_DIR/pom.xml.template | \
  sed "s/\$dp_schema_version/$dp_schema/g" | \
  sed "s/\$dp_config_version/$dp_config/g" > $SCRIPT_DIR/pom.xml

  mvn dependency:copy-dependencies -DoutputDirectory=$tmp_dir -f $SCRIPT_DIR/pom.xml
  gathering_jars $tmp_dir $target_dir

  rm $SCRIPT_DIR/pom.xml
  rm -r $tmp_dir
}


function mk_add_jar_sql() {
  local target_dir=$1
  local hql=$target_dir/add_jar.hql
  local hql_head="add jar hdfs://$2"

  if [ -e $hql ]
  then echo -n > $hql
  else touch $hql
  fi

  local fn
  local files=$(find $target_dir -name *.jar | grep latest | cut -b $((${#target_dir}+2))- )
  for fn in ${files[@]}
  do
    echo "$hql_head/${fn};" >> $target_dir/add_jar.hql
  done
}


function get_jars() {
  local input="$SCRIPT_DIR/versions.sh"
  local output=`date "+%Y%m%d"`
  local hdfs_mds_lib=''
  if [ -f $SCRIPT_DIR/hdfs.sh ]; then . $SCRIPT_DIR/hdfs.sh; fi

  while getopts i:o:l:h OPT
  do
    case $OPT in
      i)  input="$OPTARG"  ;;
      o)  output="$OPTARG" ;;
      l)  hdfs_mds_lib="$OPTARG" ;;
      h)  show_usage ;;
      \?) show_usage ;;
    esac
  done
  shift $((OPTIND - 1))

  local target_dir=$SCRIPT_DIR/jars/$output
  mkdir -p $target_dir

  if [ -d $input ]; then
    gathering_jars $(cd $input && pwd) $target_dir '-SNAPSHOT'
  else
    if [ -f $input ]; then . $input; fi
    get_jars_from_mvn $target_dir
  fi

  if [ ! -z $hdfs_mds_lib ]; then
    mk_add_jar_sql $target_dir $hdfs_mds_lib
  fi
}


function upload_to() {
  local target_server=$1
  local target_dir=$SCRIPT_DIR/jars/${2:-`date "+%Y%m%d"`}
  if [ ! -d $target_dir ]; then
    echo "$target_dir is not exist"
    show_usage
  fi

  cp $SCRIPT_DIR/deliver_jar.sh $target_dir
  if [ -f $SCRIPT_DIR/hdfs.sh ]
  then
    cp $SCRIPT_DIR/hdfs.sh $target_dir
  fi

  rsync -arv $target_dir $target_server:~/jars
}


case "$com" in
  "" ) show_usage ;;
  "list" ) get_version_list ;;
  "get"  ) get_jars $* ;;
  "to"   ) upload_to $* ;;
  * ) echo "$com is not command"; show_usage ;;
esac

