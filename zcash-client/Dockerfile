FROM debian:stretch-slim
LABEL maintainer="Haaroon Yousaf (h.yousaf [at] ucl.ac.uk)"

################## BEGIN INSTALLATION ######################
RUN apt-get update && apt-get install -y wget apt-transport-https gnupg curl wget screen


RUN wget -qO - https://apt.z.cash/zcash.asc | apt-key add -
RUN echo "deb [arch=amd64] https://apt.z.cash/ jessie main" | tee /etc/apt/sources.list.d/zcash.list
RUN apt-get update && apt-get -y install zcash=1.0.15
RUN mkdir /root/.zcash-params && mkdir /root/.zcash
RUN zcash-fetch-params
VOLUME ["/root/.zcash"]
EXPOSE 8331
CMD zcashd -daemon -rest -conf=/root/.zcash/zcash.conf && bash
