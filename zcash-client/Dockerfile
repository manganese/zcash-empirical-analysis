FROM bitnami/minideb:stretch
LABEL maintainer="rainer.stuetz@ait.ac.at"

RUN mkdir -p /root/.zcash && mkdir -p /root/zcashsrc

ADD docker/zcash.conf /root/.zcash/.zcash.conf
ADD docker/Makefile /root/zcashsrc/Makefile

RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        automake \
        autoconf \
        build-essential \
        bsdmainutils \
        ca-certificates \
        curl \
        git \
        g++-multilib \
        libc6-dev \
        libevent-dev \
        libgomp1 \
        libssl-dev \
        libtool \
        m4 \
        pkg-config \
        procps \
        unzip \
        wget \
        zlib1g-dev && \
    cd /root/zcashsrc; make install && \
    apt-get autoremove -y --purge \
        autoconf \
        automake \
        build-essential \
        libevent-dev \
        libgcc-6-dev \
        libssl-dev \
        perl \
        zlib1g-dev && \
    chown -R root:root /root/.zcash-params/ && \
    rm -rf /root/zcashsrc/src

VOLUME ["/root/.zcash"]
EXPOSE 8331

CMD zcashd -daemon -rest -conf=/root/.zcash/zcash.conf && bash
