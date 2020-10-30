# kafka-comsumer

WIP project to build my knowledge around Python and Kafka. Scrapes text files from the open text website `www.gutenberg.org`

Uses Kafka to process the messages. Only the producer side has been implemented so far. A later version will implement the consumer which will produce some more detailed statistics based on those texts

## Usage

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start the Zookeeper/Kafka stack
docker-compose up -d

# Run the Python script to start scraping the site
python kafka_producer.py
```

Example output:
```
% python kafka_producer.py
Starting Query...
=================
Looking for 4 links from http://www.gutenberg.org/robot/
Retrieved 100 links; Current_uri: harvest?filetypes[]=txt&langs[]=en; Next_uri: harvest?offset=40532&filetypes[]=txt&langs[]=en; Query_time: 0.274518s
100 links retrieved
Configured to process 4 archive(s)

> Processing archive: http://aleph.gutenberg.org/etext02/comed10.zip
Download complete (364544 bytes). Unpacking...
Finished unpacking
There are 28 file(s) matching the pattern '*.txt'
WARN: Skipping archive. Multiple matching files in archive are currently not supported

> Processing archive: http://aleph.gutenberg.org/1/2/3/7/12370/12370-8.zip
Download complete (217088 bytes). Unpacking...
Finished unpacking
There are 1 file(s) matching the pattern '*.txt'
WARN: Unable to decode file: /var/folders/gz/s1ktsfss67q56khr5bm9lnjm0000gp/T/tmpu4gm036i/12370-8.txt
Unable to retrieve the book title from the file '/var/folders/gz/s1ktsfss67q56khr5bm9lnjm0000gp/T/tmpu4gm036i/12370-8.txt' using prefix '^Title:'

> Processing archive: http://aleph.gutenberg.org/1/2/3/7/12370/12370.zip
Download complete (217088 bytes). Unpacking...
Finished unpacking
There are 1 file(s) matching the pattern '*.txt'
Current file: /var/folders/gz/s1ktsfss67q56khr5bm9lnjm0000gp/T/tmpaj2fblyf/12370.txt
Book Title: Bagh O Bahar, Or Tales of the Four Darweshes
Waiting until all the messages have been delivered to the broker...

> Processing archive: http://aleph.gutenberg.org/1/2/3/7/12372/12372-8.zip
Download complete (196608 bytes). Unpacking...
Finished unpacking
There are 1 file(s) matching the pattern '*.txt'
WARN: Unable to decode file: /var/folders/gz/s1ktsfss67q56khr5bm9lnjm0000gp/T/tmp9ss8m3mc/12372-8.txt
Unable to retrieve the book title from the file '/var/folders/gz/s1ktsfss67q56khr5bm9lnjm0000gp/T/tmp9ss8m3mc/12372-8.txt' using prefix '^Title:'


Producer Statistics
====================

Successful tasks:
===================
Bagh O Bahar, Or Tales of the Four Darweshes (/var/folders/gz/s1ktsfss67q56khr5bm9lnjm0000gp/T/tmpaj2fblyf/12370.txt)

Failed tasks:
===================
URL: http://aleph.gutenberg.org/etext02/comed10.zip
URL: http://aleph.gutenberg.org/1/2/3/7/12370/12370-8.zip
URL: http://aleph.gutenberg.org/1/2/3/7/12372/12372-8.zip

Successfully processed 1 archives
Failed to process 3 archives

Complete!
```