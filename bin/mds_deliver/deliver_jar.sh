#!/usr/bin/env sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCRIPT_DIR=$(cd $(dirname $0); pwd)
com=$1

target=current
tmp_dir_base=/tmp/common
tmp_dir=$tmp_dir_base/$USER/.tmp_mds_jar
hdfs_mds_lib='/lib'
if [ -f $SCRIPT_DIR/hdfs.sh ]; then . $SCRIPT_DIR/hdfs.sh; fi

modules=(
  mds
  dp_config
  dp_schema
)

function show_usage() {
cat << EOS
  $0 com
  command:
  set:    deploy jars in local to latest directories in HDFS
  deploy: copy latest directories to current directories
  revert: revert current directories from current.bak in HDFS
EOS
  exit 0
}

function com_hdfs() {
  if [ -z $hdfs_user ]
  then
    echo "hdfs dfs $*"
    hdfs dfs $*
  else
    echo "sudo -u $hdfs_user hdfs dfs $*"
    sudo -u $hdfs_user hdfs dfs $*
  fi
}

function set_to_latest() {
  local src_dir=$(cd $(dirname $0); pwd)

  # temporaly move jar files to /tmp/common/$USER/ to be able to access from $hdfs_user user
  local base_name=`basename $src_dir`
  local dir_name=`dirname $src_dir`

  mkdir -p  $tmp_dir_base
  chmod 777 $tmp_dir_base
  mkdir -p  $tmp_dir
  cp -r $src_dir $tmp_dir

  local module
  for module in ${modules[@]}; do
    local tdn=$tmp_dir/$base_name/$module
    local versions=`ls $tdn`
    local version
    for version in ${versions[@]}; do
      com_hdfs -rm -r -skipTrash $hdfs_mds_lib/$module/$version
      com_hdfs -put $tdn/$version $hdfs_mds_lib/$module/$version
    done
  done
  rm -r $tmp_dir
}


function deploy_to_current() {
  # back up current dir to .bak 
  # move latest directory to current for fast changing

  local module
  for module in ${modules[@]}; do
    com_hdfs -rm -r -skipTrash $hdfs_mds_lib/$module/$target.bak
  done
  for module in ${modules[@]}; do
    com_hdfs -mv $hdfs_mds_lib/$module/$target $hdfs_mds_lib/$module/$target.bak
  done
  for module in ${modules[@]}; do
    com_hdfs -mv $hdfs_mds_lib/$module/latest $hdfs_mds_lib/$module/$target
  done
  for module in ${modules[@]}; do
    com_hdfs -cp $hdfs_mds_lib/$module/$target $hdfs_mds_lib/$module/latest
  done
}


function revert_deploy() {
  local module
  for module in ${modules[@]}; do
    com_hdfs -rm -r -skipTrash $hdfs_mds_lib/$module/$target
  done
  for module in ${modules[@]}; do
    com_hdfs -mv $hdfs_mds_lib/$module/$target.bak $hdfs_mds_lib/$module/$target
  done
}


case "$com" in
  "" ) show_usage ;;
  "set"    ) set_to_latest ;;
  "deploy" ) deploy_to_current ;;
  "revert" ) revert_deploy ;;
  * ) echo "$com is not command"; show_usage ;;
esac

