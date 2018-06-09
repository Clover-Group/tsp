FROM registry.itclover.ru/clover/streammachine:core
MAINTAINER Clover DevOps <devops@itclover.ru>

ADD ./ /code
ADD ./docker-app/docker-entrypoint.sh /docker-entrypoint.sh

ENV JAVA_VERSION_MAJOR=8 \
    JAVA_VERSION_MINOR=131 \
    JAVA_VERSION_BUILD=11 \
    JAVA_URL_HASH=d54c1d3a095b4ff2b6607d096fa80163

ENV JAVA_VERSION = 1.$JAVA_VERSION_MAJOR.0_$JAVA_VERSION_MINOR

RUN yum update -y && \
    yum install -y wget && \
    wget --no-cookies --no-check-certificate \
     --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
     "http://download.oracle.com/otn-pub/java/jdk/${JAVA_VERSION_MAJOR}u${JAVA_VERSION_MINOR}-b${JAVA_VERSION_BUILD}/${JAVA_URL_HASH}/jdk-${JAVA_VERSION_MAJOR}u${JAVA_VERSION_MINOR}-linux-x64.rpm"  && \
    yum localinstall -y jdk-${JAVA_VERSION_MAJOR}u${JAVA_VERSION_MINOR}-linux-x64.rpm && \
    rm -f jdk-${JAVA_VERSION_MAJOR}u${JAVA_VERSION_MINOR}-linux-x64.rpm && \
    rm -rf /var/cache/yum

RUN curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo
ENV	JAVA_HOME=/usr/java/jdk$1.${JAVA_VERSION_MAJOR}.0_${JAVA_VERSION_MINOR}/


RUN chmod +x /code/start.sh
RUN chmod +x /docker-entrypoint.sh

WORKDIR /code

RUN /code/start.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
