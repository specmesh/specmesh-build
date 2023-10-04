FROM eclipse-temurin:11-jre-focal@sha256:4c1ab549795f20b661bfaaa22716e5e10a8eae66a534e668a1e028683c810efa

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