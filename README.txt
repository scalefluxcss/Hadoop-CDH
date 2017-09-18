# Hadoop-2.6.0-CDH5.4.0
This repository contains files and instructions to integrate CSSZlib compression to Hadoop-2.6.0-CDH5.4.0.

## Prerequisite
- Make sure Scaleflux CSS card is installed
- Make sure Scaleflux driver rpm is installed via sfxinstall.sh
- Make sure Cloudera CDH 5.4.0 is installed
- Make sure CDH runner has permission to RW /dev/sfx0

## Build Jar packages for Hadoop-2.6.0-CDH5.4.0
- Git clone cdh5-2.6.0_5.4.0 with:
         git clone -b cdh5-2.6.0_5.4.0 https://github.com/scalefluxcss/Hadoop-CDH.git
- Or download zip package:
         https://github.com/scalefluxcss/Hadoop-CDH/archive/cdh5-2.6.0_5.4.0.zip
- Refer to BUILDING.txt for more details about library dependencies and build commands.
- After dependencies were all installed, compile packages with maven command:
         mvn package -DskipTests
