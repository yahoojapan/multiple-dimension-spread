<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Object
create MDS jar files from Maven repositories.

# How to use
## check mds related modules versions on Maven repository
Use following command.

    local$ ./get_jar.sh list

## get jars from maven repository
Use following command.

    local$ ./get_jar.sh get [-i versions.sh] [-o target_dir] [-l hdfs_lib]

### versions.sh
MDS jar set for hive consists of mds, dp\_schema and dp\_config modules.
Firstly, copy versions.sh from versions.template.

    local$ cp versions.sh.template versions.sh

Then, you can describe the versions in this file ("versions.sh") for each module,

    mds=${mds_version}
    dp_schema=${dp_schema_version}
    dp_config=${dp_config_version}

When the module versions are not defined, release versions are used.

### target\_dir
Downloaded jar files are set in jars/target\_dir.
Default target\_dir is YYYYMMDD created using date command.


## deploy
### up to server
Upload the downloaded jar files and deliver script to target server.

    local$ ./get_jar.sh to target_server [target_dir]

Default target\_dir is YYYYMMDD created using date command.
Then, jars dir is made on your home directory in the server.

    local$ ssh target_server
    server$ cd jars/$target_dir

### set to latest directory
Set hive jars to "latst" directory in HDFS.

    server$ ./deliver_jar set [source_dir]

Default source\_dir is YYYYMMDD created using date command.

### check
Check whethre data can access through delivered mds jars.
Connect hive or beeline to target hdfs and check data.

    server$ sudo -u specific_user_if_needed beeline -i add_jar.hql

### deploy
Deploy to "current" directory from "latest".

    server$ ./deliver_jar deploy

### revert
If something is wrong, you can revert "current" from backup directory.

    server$ ./deliver_jar revert

