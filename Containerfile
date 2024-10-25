FROM --platform=linux/arm64 python:slim

RUN apt-get update && \
    apt-get install -y gcc make wget unzip swig

RUN pip install setuptools

RUN wget http://abyz.me.uk/lg/lg.zip
RUN unzip lg.zip
RUN cd lg; make && make install

RUN pip install lgpio RPi.GPIO gpiozero paho-mqtt tinydb

COPY . .

CMD ["python", "-u", "shutter-control.py"]