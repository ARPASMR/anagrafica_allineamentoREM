FROM arpasmr/r-base
RUN apt-get install -y s3cmd
COPY . /usr/local/src/myscripts
WORKDIR /usr/local/src/myscripts
CMD ["./verifiche.sh"]
