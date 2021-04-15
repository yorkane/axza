ARG ENABLE_PROXY=true

FROM openresty/openresty:1.19.3.1-alpine-fat AS production-stage

ARG ENABLE_PROXY
ARG APISIX_PATH
RUN set -x \
	&& (test "${ENABLE_PROXY}" != "true" || /bin/sed -i 's,http://dl-cdn.alpinelinux.org,https://mirrors.aliyun.com,g' /etc/apk/repositories) \
	&& apk add --no-cache --virtual .builddeps jq \
	automake \
	autoconf \
	libtool \
	pkgconfig \
	cmake \
	git \
	&&  ln -sf /usr/local/apisix/bin/apisix /usr/bin/apisix

WORKDIR /usr/local/apisix
CMD ["tail", "-f", "/dev/stdout"]

# docker build -t apdev:1 ./ -f dev.Dockerfile
# docker run -v `pwd`:/usr/local/apisix --network=host
# /usr/bin/apisix init && /usr/bin/apisix init_etcd && /usr/local/openresty/bin/openresty -p /usr/local/apisix -g 'daemon off;'

# make deps
#mkdir logs
# ln -sf /usr/local/apisix/bin/apisix /usr/bin/apisix
# /usr/bin/apisix init && /usr/bin/apisix init_etcd && /usr/local/openresty/bin/openresty -p /usr/local/apisix 
