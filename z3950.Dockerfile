FROM ubuntu:trusty
RUN apt update && apt install -y curl python-pip
RUN curl 'https://pypi.python.org/packages/6a/34/8176b841926a2add20524a9f74c307ac5fe6e33e9f4af12a58e6f7223982/mollyZ3950-2.04-molly1.tar.gz#md5=a0e5d7bb395ae31026afc7f974711630' > mollyZ3950-2.04-molly1.tar.gz
# sudo pip2 install ./mollyZ3950-2.04-molly1.tar.gz
# sudo pip2 install pymarc
