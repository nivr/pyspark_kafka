ARG debian_buster_image_tag=8-jre-slim-bullseye
FROM openjdk:${debian_buster_image_tag}

# -- Layer: OS + Python 3.7

ARG shared_workspace=/opt/workspace

RUN mkdir -p ${shared_workspace} && \
    apt-get update -y && \
	apt install -y curl gcc &&\ 
	apt install -y build-essential zlib1g-dev libncurses5-dev && \
	apt install -y libsqlite3-dev && \
	apt install -y libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget libjpeg-dev && \
	apt install -y python3 python3-pip && \
    apt-get update && apt-get install -y procps && apt-get install -y vim && apt-get install -y net-tools && \
	apt-get install -y nano && \
	rm -rf /var/lib/apt/lists/*

ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]
