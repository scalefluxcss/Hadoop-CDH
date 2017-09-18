# Hadoop-2.6.0-CDH5.4.0
This repository contains files and instructions to integrate CSSZlib compression to Hadoop-2.6.0-CDH5.4.0.


## Prerequisite
- Make sure Scaleflux CSS card is installed
- Make sure Scaleflux driver rpm is installed via sfxinstall.sh
- Make sure Cloudera CDH 5.4.0 is installed
- Make sure CDH runner has permission to RW /dev/sfx0


## Create soft links to CSSZlib Compression native libraries
If Cloudera CDH 5.4.0 is installed at /opt/cloudera/parcels/CDH, let’s say $CDH_HOME=/opt/cloudera/parcels/CDH

- Create soft links on CentOS:

         ln -s /usr/lib64/libcsszjni.so $CDH_HOME/lib/hadoop/lib/native/libcsszjni.so
         ln -s /usr/lib64/libcssz.so $CDH_HOME/lib/hadoop/lib/native/libcssz.so

- Create soft links on Ubuntu:

         ln -s /usr/lib/x86_64-linux-gnu/libcsszjni.so $CDH_HOME/lib/hadoop/lib/native/libcsszjni.so
         ln -s /usr/lib/x86_64-linux-gnu/libcssz.so $CDH_HOME/lib/hadoop/lib/native/libcssz.so


## Build Jar packages for Hadoop-2.6.0-CDH5.4.0
- Git clone cdh5-2.6.0_5.4.0 with:

         git clone -b cdh5-2.6.0_5.4.0 https://github.com/scalefluxcss/Hadoop-CDH.git
- Or download zip package:

         https://github.com/scalefluxcss/Hadoop-CDH/archive/cdh5-2.6.0_5.4.0.zip
- Refer to BUILDING.txt for more details about library dependencies and build commands.
- After dependencies were all installed, compile packages with maven command:

         mvn package -DskipTests


## Replace Jar packages to enable CSSZlib Compression
- Replaces three Jar packages within $CDH_HOME/jars:

         hadoop-common-2.6.0-cdh5.4.0.jar
         hadoop-mapreduce-examples-2.6.0-cdh5.4.0.jar

- Replace hadoop-common-2.6.0-cdh5.4.0.jar within $CDH_HOME/lib/hadoop
