FROM registry.itclover.ru/clover/streammachine:core
MAINTAINER Clover DevOps <devops@itclover.ru>

ADD ./ /code
ADD ./docker-app/docker-entrypoint.sh /docker-entrypoint.sh

RUN curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo

WORKDIR /code

RUN yum install git -y
RUN chmod +x /code/compile-jar.sh
RUN chmod +x /code/start-jar-local.sh
RUN chmod +x /code/start-jar-cluster.sh
RUN chmod +x /docker-entrypoint.sh

RUN /code/compile-jar.sh

ENTRYPOINT ["/docker-entrypoint.sh"]