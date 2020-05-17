"""
Kafka producer which sends lines from text files for processing
"""

from confluent_kafka import Producer, KafkaException
import sys
import re
import json
import requests
from bs4 import BeautifulSoup
import zipfile
import os
import fnmatch
import tempfile


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
        print("% Message failed delivery {}".format(err), file=sys.stderr)
    # else:
    #     # success
    #     print("% Message delivered to {} [{}] @ {}".format(msg.topic(), msg.partition(), msg.offset()))


def produce_kafka_message(client, topic, message):
    """
    Produces a single message to a Kafka topic

    :param client: handle to the Kakfa Producer object
    :param topic: which Kafka topic to write to
    :param message: the message to write to the Kafka topic

    :return None
    """
    try:
        client.produce(topic, message, callback=delivery_report)
        return None

    except KafkaException as e:
        print("% Kafka exception: {}".format(e), file=sys.stderr)

    except BufferError:
        print("% Local producer queue is full ({} messages awaiting delivery): try again".format(len(p)))


def read_text_file_to_list(path):
    """
    Reads a text file and returns a list of the lines

    :param path: path to the text file

    :return list containing each line as an item. Return False if unable to complete successfully
    """
    try:
        file = open(path, "r")
        # Reads all lines and splits to list
        return file.readlines()

    except FileNotFoundError:
        print("WARN: Unable to find file: {}".format(path))
        return False

    except UnicodeDecodeError:
        print("WARN: Unable to decode file: {}".format(path))
        return False


def find_book_title_in_text_file(path, regex_prefix="^Title:"):
    """
    Retrieves the book title from a text file
    Assumes the title is always prefixed with the regex_prefix, and in the top section, as with Project Gutenberg
    Uses the regex to locate the line containing the title. Ensures only a single entry is matched

    :param path: path to the text file
    :param regex_prefix: regex for describing the string prefix before the title information in the file

    :return False on no match or multiple matches. On a single match the string containing the title is returned
    """

    title_line = None           # the title to return
    count_matched = 0           # the number of matches. Ensures we only have one match for consistency

    lines = read_text_file_to_list(path)
    # Check for a successful response
    if lines:
        # Iterate through the lines looking for the one containing the string prefix
        # We only need to use a slice as the title information is always included in the top section of the text file
        for line in lines[0:100]:
            if re.search(regex_prefix, line, flags=re.IGNORECASE):
                # Matching line found
                length_of_string_prefix = len(regex_prefix)     # used to determine the start position of the title
                title_line = line[length_of_string_prefix:]     # remove the prefix from the string
                count_matched += 1

        # Check whether we have found a match
        if count_matched == 1:
            # Found exactly one match - success
            return title_line
        else:
            # no matches or multiple matches - failure
            return False
    else:
        # Unable to establish book title
        return False


def process_book_lines(client, file_path, prefix, kafka_topic):
    """
    Process the individual lines in the book text file
    Filter out just newline lines.
    Produce Kafka messages based on the book title and current line
    These messages will be consumed by a downstream Kafka client

    :param client: handle to the Kafka Producer object
    :param file_path: path to the text file (book)
    :param prefix: regex for describing the string prefix before the title information in the file
    :param kafka_topic:which Kafka topic to write to
    :return Dict containing book path, name and some stats. Empty dict on failure
    """

    # Read from text file. Attempt to find book title in the text
    book_title = find_book_title_in_text_file(file_path, prefix)

    if book_title:
        print("Current file: {}\nBook Title: {}".format(file_path, book_title), end="")       # end=""

        # Split the book into a list of strings (one per line)
        text_file_contents_list = read_text_file_to_list(file_path)
        if not text_file_contents_list:
            print("Unable to open file: {}".format(file_path))
            return {}
        total_number_of_lines = len(text_file_contents_list)

        # Iterate through the book lines, create a json representation of a dict for each, and produce a Kafka message
        newline_lines = 0           # counter for lines containing just newline characters
        sent_kafka_messages = 0     # counter for messages sent to Kafka
        for line in text_file_contents_list:

            # Remove any lines just containing newline characters
            if line != "\n":
                kafka_message_json = json.dumps({
                    "book_title": book_title,
                    "book_line": line
                })

                # Send message (async)
                produce_kafka_message(client=client, topic=kafka_topic, message=kafka_message_json)

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
        print("Unable to retrieve the book title from the file '{}' using prefix '{}'. "
              "No kafka messages to send".format(file_path, prefix))
        return {}


def display_producer_stats(result_object):
    """
    Display some stats generated by the producer
    :param result_object: Dict containing the producer results after sending Kafka messages
    :return: None
    """

    # Display some producer stats
    print("File: {}\nBook Title: {}".format(result_object["file_path"], result_object["book_title"]), end="")
    print("Total number of lines in this book: {}".format(result_object["total_number_of_lines"]))
    print("Number of newline only lines: {}".format(result_object["total_just_newline_lines"]))
    print("Number of sent Kafka messages: {}".format(result_object["sent_kafka_messages"]))
    print("")


def extract_href_links_from_html_page(page, regex="^http.+zip$"):
    """
    Extracts the href links from a HTML page. Ensures the URLs match a particular format to avoid invalid links

    :param page: the raw HTML page containing the links
    :param regex: regex used to help filter out partial links. Defaults to ^http.+zip$
    :return: list of hrefs extracted from the page
    """

    # Extract the href links from the HTML page & add to a list
    extracted_href_links = []
    soup = BeautifulSoup(page, "html.parser")

    for link in soup.find_all("a"):
        if re.search(regex, link.get("href")):
            extracted_href_links.append(link.get("href"))

    return extracted_href_links


def find_files_in_directory(pattern, path):
    """
    Returns a list of files which match a filename pattern within the directory path, including subdirectories
    :param pattern: shell glob style pattern to match against filenames
    :param path: the base directory to start the search from
    :return: list of files which match the pattern. Return 0 if it isn't a directory
    """
    # Check the path is a directory
    if os.path.isdir(path):
        found_files = []
        # Iterate through all directories (inc. sub directories), and compare the filename against the pattern
        for root, dirs, files in os.walk(path):
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    found_files.append(os.path.join(root, name))
        return found_files
    else:
        print("ERR: {} is not a directory".format(path))
        return 0


if __name__ == "__main__":

    # Variables todo: extract as environment variables
    num_books_to_process = 2

    kafka_client_config = {"bootstrap.servers": "localhost:29092,localhost:29093"}
    topic = "important-topic"
    title_regex_prefix = "^Title:"
    filename_glob_pattern = "*.txt"
    book_source_download_url = "http://www.gutenberg.org/robot/harvest?offset=40536&filetypes[]=txt&langs[]=en"
    # book_source_download_url = "http://www.gutenberg.org/robot/harvest?filetypes[]=txt&langs[]=en"
    list_of_sending_stats = []
    list_of_failed_to_process_books = []

    # Download HTML page containing a paginated list of (English) raw text (txt) files from Project Gutenberg
    print("Requesting the (paginated) list of book archive files from: {}".format(book_source_download_url))
    r = requests.get(book_source_download_url)
    html_page = r.text
    response_time = r.elapsed.total_seconds()

    # Extract href links from HTML page
    archive_links_list = extract_href_links_from_html_page(html_page)

    # Download and un-archive files
    if len(archive_links_list) > 0:
        print("{} links retrieved".format(len(archive_links_list)))
        print("Server response time: {} seconds".format(response_time))
        print("Configured to process {} archive(s)".format(num_books_to_process))

        # Iterate over the requested number of archives
        for url in archive_links_list[:num_books_to_process]:
            print("\n> Processing archive: {}".format(url))

            # Steam the download to temporary file
            r = requests.get(url, stream=True)

            with tempfile.TemporaryFile(mode='w+b') as temp_archive_name:   # read + write + binary mode
                for chunk in r.iter_content(chunk_size=128):
                    temp_archive_name.write(chunk)
                # Check a file is now present and it contains data
                if os.stat(temp_archive_name.name).st_size > 0:
                    print("Download complete ({} bytes). Unpacking...".format(os.stat(temp_archive_name.name).st_size))
                else:
                    print("ERR: There was a problem downloading the archive file: {}".format(temp_archive_name.name))

                # Unpack the archive to a temp directory
                with tempfile.TemporaryDirectory() as temp_dir_name:
                    with zipfile.ZipFile(temp_archive_name, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir_name)
                    print("Finished unpacking. Temp files are located in: {}".format(temp_dir_name))

                    # Find all the txt files which have been unpacked
                    list_of_txt_files = find_files_in_directory(filename_glob_pattern, temp_dir_name)

                    # Output how many matching files have been found in the archive
                    print("There are {} file(s) matching the pattern '{}'"
                          .format(len(list_of_txt_files), filename_glob_pattern))

                    # Only one file found in the archive matching the pattern. Currently the only supported route
                    if len(list_of_txt_files) == 1:
                        # Create Kafka Producer client
                        p = Producer(kafka_client_config)

                        # Process messages
                        result = process_book_lines(client=p, file_path=list_of_txt_files[0],
                                                    prefix=title_regex_prefix, kafka_topic=topic)

                        # Successful book title lookup and messages have been sent to kafka
                        if result:
                            print("Waiting until all the messages have been delivered to the broker...")
                            p.flush()

                            # Add to list to allow us to display stats at the end
                            list_of_sending_stats.append(result)

                        # Keep track of books we failed to process
                        else:
                            list_of_failed_to_process_books.append({"url": url})

                    # Multiple files matching the pattern in the archive - skipping as not currently supported
                    elif len(list_of_txt_files) > 1:
                        print("WARN: Skipping archive. Multiple matching files in archive are currently not supported")
                        list_of_failed_to_process_books.append({"url": url})

                    # No files matching the pattern in the archive
                    else:
                        print("WARN: Skipping archive. No matching files found")
                        list_of_failed_to_process_books.append({"url": url})

        # Display stats
        print("\nFinished Sending. Show statistics\n==============================")
        for s in list_of_sending_stats:
            display_producer_stats(s)

        print("Totals:\n========")
        print("Successfully processed {} archives\nFailed to process {} archives"
              .format(len(list_of_sending_stats), len(list_of_failed_to_process_books)))

        print("\nComplete!")

    else:
        print("ERR: No links were returned. Exiting")
        sys.exit(2)
