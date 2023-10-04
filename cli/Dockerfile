FROM amazoncorretto:20@sha256:ac32b3e93f843470c7f55b7d694df016def3d02d55de83e2f43d432a8884b3a4

ARG APP_NAME
ARG APP_VERSION
ENV VERSION=$APP_VERSION

LABEL org.opencontainers.image.source=https://github.com/specmesh/specmesh-build/tree/main/cli

RUN mkdir -p /opt/specmesh

COPY bin /bin
COPY log4j /log

RUN chmod +x /bin/*.sh

COPY ${APP_NAME}-${APP_VERSION} /opt/specmesh/${APP_NAME}-${APP_VERSION}
WORKDIR /opt/specmesh

RUN ln -s ${APP_NAME}-${APP_VERSION} service

ENTRYPOINT ["/bin/wrapper.sh"]
#No CMD instruction is provided, allowing you to pass any number of arguments at runtime.