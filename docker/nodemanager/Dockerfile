FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
USER root
RUN sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list
RUN apt-get update -y && apt-get install -y wget bzip2
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-py311_23.11.0-2-Linux-x86_64.sh -O /tmp/miniconda.sh
RUN bash /tmp/miniconda.sh -b -p /opt/miniconda
RUN /opt/miniconda/bin/conda install -y python=3.11
RUN ln -sf /opt/miniconda/bin/python3.11 /usr/bin/python3
RUN rm /tmp/miniconda.sh