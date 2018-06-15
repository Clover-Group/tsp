FROM registry.itclover.ru/clover/streammachine:core
MAINTAINER Clover DevOps <devops@itclover.ru>

ADD ./ /code
ADD ./docker-app/docker-entrypoint.sh /docker-entrypoint.sh

RUN curl https://bintray.com/sbt/rpm/rpm | tee /etc/yum.repos.d/bintray-sbt-rpm.repo

WORKDIR /code

RUN chmod +x /code/compile.sh
RUN chmod +x /code/start.sh
RUN chmod +x /docker-entrypoint.sh

RUN /code/compile.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
