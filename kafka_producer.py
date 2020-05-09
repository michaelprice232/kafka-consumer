"""
Kafka producer which sends lines from text files for processing
"""

from confluent_kafka import Producer, KafkaException
import sys
import re
import json


def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result (to brokers)
    Triggered by poll() or flush()

    :param err: contains error information from callbacks
    :param msg: contains information from callbacks

    :return None
    """

    if err:
        # error
        print('% Message failed delivery {}'.format(err), file=sys.stderr)
    else:
        # success
        print('% Message delivered to {} [{}] @ {}\n'.format(msg.topic(), msg.partition(), msg.offset()))


def produce_kafka_message(client, topic, message, delivery_handler):
    """
    Produces a single message to a Kafka topic

    :param client: handle to the Kakfa Producer object
    :param topic: which Kafka topic to write to
    :param message: the message to write to the Kafka topic
    :param delivery_handler: which function to use for delivery receipts

    :return None
    """
    try:
        client.produce(topic, message, callback=delivery_handler)
        return None

    except KafkaException as e:
        print('% Kafka exception: {}'.format(e), file=sys.stderr)

    except BufferError:
        print('% Local producer queue is full ({} messages awaiting delivery): try again\n'.format(len(p)))


def read_text_file_to_list(path):
    """
    Reads a text file and returns a list of the lines

    :param path: path to the text file

    :return list containing each line as an item
    """

    file = open(path, 'r')
    # Reads all lines and splits to list
    return file.readlines()


def find_book_title_in_text_file(path, regex_prefix='^Title:'):
    """
    Retrieves the book title from a text file
    Assumes the title is always prefixed with the regex_prefix, and in the top section, as with Project Gutenberg
    Uses the regex to locate the line containing the title. Ensures only a single entry is matched

    :param path: path to the text file
    :param regex_prefix: regex for describing the string prefix before the title information in the file

    :return False on no match or multiple matches. On a single match the string containing the title is returned
    """

    title_line = None           # the title to return
    line_matched = False        # track whether we have matched the line or not
    count_matched = 0           # the number of matches. Ensures we only have one match for consistency
    lines = read_text_file_to_list(path)

    # Iterate through the lines looking for the one containing the string prefix
    # We only need to use a slice as the title information is always included in the top section of the text file
    for line in lines[0:40]:
        if re.search(regex_prefix, line, flags=re.IGNORECASE):
            # Matching line found
            length_of_string_prefix = len(regex_prefix)
            title_line = line[length_of_string_prefix:]     # remove the prefix from the string
            line_matched = True
            count_matched += 1

    # Check whether we have found a match
    if line_matched and count_matched == 1:
        # Found exactly one match - success
        return title_line
    else:
        return False


def process_book_lines(client, file_path, prefix, kafka_topic):
    """
    Process the individual lines in the book text file
    Filter out just newline lines. Produce Kafka messages based on the book title and current line
    These messages will be consumed by a downstream Kafka client

    :param client: handle to the Kakfa Producer object
    :param: file_path: path to the text file (book)
    :param prefix: regex for describing the string prefix before the title information in the file
    :param kafka_topic:which Kafka topic to write to

    :return Dict containing book path, name and some stats (# lines, and # newline character lines)
    """

    # Read from text file. Attempt to find book title in the text. todo: read books from web
    book_title = find_book_title_in_text_file(file_path, prefix)

    if book_title:
        print("Current file: {}\nBook Title: {}\n".format(file_path, book_title), end="")

        # Split the book into a list of strings (one per line)
        text_file_contents_list = read_text_file_to_list(file_path)
        total_number_of_lines = len(text_file_contents_list)

        # Iterate through the book lines, create a json representation of a dict for each, and produce a Kafka message
        newline_lines = 0           # counter for lines containing just newline characters
        sent_kafka_messages = 0     # counter for messages sent to Kafka
        for line in text_file_contents_list:

            # Remove any lines just containing newline characters
            if line != '\n':
                kafka_message_json = json.dumps({
                    "book_title": book_title,
                    "book_line": line
                })

                # Send message (async)
                produce_kafka_message(client=client, topic=kafka_topic, message=kafka_message_json,
                                      delivery_handler=delivery_report)

                sent_kafka_messages += 1

            else:
                # the line is just a newline character. Don't send it, just record the count
                newline_lines += 1

        return {
            "book_title": book_title,
            "file_path": file_path,
            "total_number_of_lines": total_number_of_lines,
            "total_just_newline_lines": newline_lines,
            "sent_kafka_messages": sent_kafka_messages
        }

    else:
        raise Exception("Unable to retrieve the book title from the text".format(file_path, prefix))


def display_producer_stats(result_object):
    """
    Display some stats generated by the producer

    :param result_object: Dict containing the producer results after sending Kafkd messages
    :return: None
    """

    # Display some producer stats
    print("File: {}\nBook Title: {}".format(result_object["file_path"], result_object["book_title"]), end="")
    print("Total number of lines in this book: {}".format(result_object["total_number_of_lines"]))
    print("Number of newline only lines: {}".format(result_object["total_just_newline_lines"]))
    print("Number of sent Kafka messages: {}".format(result_object["sent_kafka_messages"]))


if __name__ == '__main__':

    # Variables todo: extract as environment variables
    kafka_client_config = {'bootstrap.servers': 'localhost:29092,localhost:29093'}
    topic = 'important-topic'
    poll_timeout_seconds = 2
    title_regex_prefix = '^Title:'
    text_file_path = './books/76-0.txt'

    # Create Kafka Producer client
    p = Producer(kafka_client_config)

    # Process messages
    result = process_book_lines(client=p, file_path=text_file_path, prefix=title_regex_prefix, kafka_topic=topic)

    # Wait until all the messages have been delivered to the broker
    print("Wait until all the messages have been delivered to the broker")
    p.flush()

    # Display results
    print("Show statistics\n================")
    display_producer_stats(result)

    print("\nProcessed all books. Complete!")
