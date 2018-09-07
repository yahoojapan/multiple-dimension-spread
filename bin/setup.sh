#!/bin/sh

basedir=`dirname $0`
libdir="$basedir/../jars"
dependency_libdir="$libdir/lib"
template="$basedir/../etc"

if [ -e $libdir ];then
  rm -r $libdir
fi

mkdir $libdir

sed "s/\$mds_version/0.8.2.hive-1.2.1000.2.6.2.0-205/g" $template/pom.xml.template | \
sed "s/\$dp_schema_version/1.2.1/g" | \
sed "s/\$dp_config_version/1.2.1.1/g" > $libdir/pom.xml

mvn dependency:copy-dependencies -DoutputDirectory=. -f $libdir/pom.xml

mkdir $dependency_libdir
cp $template/dependency_pom.xml.template $dependency_libdir/pom.xml

mvn dependency:copy-dependencies -DoutputDirectory=. -f $dependency_libdir/pom.xml
