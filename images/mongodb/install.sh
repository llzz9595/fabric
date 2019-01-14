#!/bin/bash
#
# Based this file on https://github.com/docker-library/mongo/blob/master/3.4/Dockerfile &
# https://docs.mongodb.com/manual/tutorial/install-mongodb-enterprise-on-ubuntu/#install-mongodb-enterprise

groupadd -r mongodb && useradd -r -g mongodb mongodb

apt-get update \
&& apt-get install -y --no-install-recommends ca-certificates jq numactl 

# grab gosu for easy step-down from root
export GOSU_VERSION=1.10

set -x \
	&& apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/* \
	&& wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" \
	&& wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc" \
	&& export GNUPGHOME="$(mktemp -d)" \
	&& gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
	&& gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
	&& rm -r "$GNUPGHOME" /usr/local/bin/gosu.asc \
	&& chmod +x /usr/local/bin/gosu \
	&& gosu nobody true

mkdir /docker-entrypoint-initdb.d

# Add GPG Keys & update apt sources

apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6

echo "deb [ arch=amd64,arm64,ppc64el,s390x ] http://repo.mongodb.com/apt/ubuntu xenial/mongodb-enterprise/3.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-enterprise.list

# tmp solution of unmet dependencies when building docker image in s390x
if [ $(dpkg --print-architecture) == "s390x" ]
then
echo "deb [ arch=s390x ] http://deb.debian.org/debian stable main" >> /etc/apt/sources.list
fi

apt-get update

export MONGO_PACKAGE=mongodb-enterprise
export MONGO_MAJOR=3.4
export MONGO_VERSION=3.4.9

apt-get install -y \
		${MONGO_PACKAGE}=$MONGO_VERSION \
		${MONGO_PACKAGE}-server=$MONGO_VERSION \
		${MONGO_PACKAGE}-shell=$MONGO_VERSION \
		${MONGO_PACKAGE}-mongos=$MONGO_VERSION \
		${MONGO_PACKAGE}-tools=$MONGO_VERSION

mkdir -p /data/db /data/configdb \
&& chown -R mongodb:mongodb /data/db /data/configdb

# Clean Up apt list
rm -rf /var/lib/apt/lists/*
