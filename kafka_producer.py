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
import time


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
        print("% Local producer queue is full ({} messages awaiting delivery): try again".format(len(client)))


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


def read_text_file_to_string(path):
    """
    Reads a text file and returns a string
    :param path: path to the text file
    :return string containing the contents of text file. Return False if unable to complete successfully
    """
    try:
        file = open(path, "r")
        return file.read()

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


def process_book_in_full(client, file_path, prefix, kafka_topic):
    """
    Process the a text file to a) determine it's title b) send the contents to a kafka broker
    These will be processed by a downstream kafka consumer

    :param client: handle to the Kafka Producer object
    :param file_path: path to the text file (book)
    :param prefix: regex for describing the string prefix before the title information in the file
    :param kafka_topic:which Kafka topic to write to
    :return Dict containing book title and path. Empty dict on failure
    """

    # Attempt to find book title in the text
    book_title = find_book_title_in_text_file(file_path, prefix)

    if book_title:
        print("Current file: {}\nBook Title: {}".format(file_path, book_title), end="")

        # Read the text file as single string. Check we could open the file and it is not empty
        full_book_as_string = read_text_file_to_string(file_path)
        if not full_book_as_string:
            print("Unable to open file: {}".format(file_path))
            return {}

        # Send Kafka message (async)
        kafka_message_json = json.dumps({
            "book_title": book_title,
            "contents": full_book_as_string
        })
        produce_kafka_message(client=client, topic=kafka_topic, message=kafka_message_json)

        return {
            "book_title": book_title,
            "file_path": file_path
        }

    else:
        print("Unable to retrieve the book title from the file '{}' using prefix '{}'. "
              "No kafka messages to send".format(file_path, prefix))
        return {}


def retrieve_archive_links(base_url_address, start_uri, required_num_links, timeout=5,
                           file_name_regex="^http.+zip$", sleep_interval=0.5):
    """
    Iterate through paginated HTML pages until the requested number of archive href links are retrieved
    :param base_url_address: Base URL which all the paginated URIs exist under
    :param start_uri: The URI for the initial iteration (without any offset query string)
    :param required_num_links: The minimum number of archive href links which need to be returned
    :param file_name_regex: Regex used to match filenames to ensure we are matching only archive files
    :param timeout: Max timeout for HTML requests.
    :param sleep_interval: time (in seconds) to rest between each HTML request. Avoids overloading the server
    :return: List containing the href links. False for any failures
    """

    links_retrieved = 0
    current_uri = start_uri
    extracted_href_links = []

    print("Looking for {} links from {}...".format(required_num_links, base_url_address))

    # Iterate until the total number of requested links has been retrieved for the paginated HTML pages
    while links_retrieved < required_num_links:
        try:
            r = requests.get(base_url_address + current_uri, timeout=timeout)
            html_page = r.text

        except requests.ConnectionError as e:
            print("Connection error. Please retry later: ".format(e))
            return False

        except requests.Timeout as e:
            print("Request timed out: {} (set to {} seconds). Please retry later".format(e, timeout))
            return False

        # Parse the HTML page for href links
        soup = BeautifulSoup(html_page, "html.parser")

        soup_links = soup.find_all("a")
        for link in soup_links:
            # Ensure the link matches our desired filename pattern
            if re.search(file_name_regex, link.get("href")):
                extracted_href_links.append(link.get("href"))

        links_retrieved = len(extracted_href_links)

        # The next page link is always the last href on the page
        next_uri = soup_links[-1].get("href")
        print("Retrieved {} links; Current_uri: {}; Next_uri: {}; Query_time: {}s"
              .format(links_retrieved, current_uri, next_uri, r.elapsed.total_seconds()))

        current_uri = next_uri
        time.sleep(sleep_interval)             # Reduce load on the remote server

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


def display_producer_stats(success_objects, failure_objects):
    """
    Displays the statistics for success and failures after processing books
    :param success_objects: List containing the return objects for successful tasks
    :param failure_objects: List containing the return objects for failed tasks
    :return: None
    """

    # Display some producer stats
    print("\n\nProducer Statistics\n====================\n")

    if success_objects:
        print("Successful tasks:\n===================")
        for success in success_objects:
            print("Title: {} (Path: {})".format(success["book_title"].rstrip(), success["file_path"]))

    if failure_objects:
        print("\nFailed tasks:\n===================")
        for failure in failure_objects:
            print("URL: {}".format(failure['url']))

    print("\nSuccessfully processed {} archives\nFailed to process {} archives"
          .format(len(success_objects), len(failure_objects)))


if __name__ == "__main__":

    # Variables todo: extract as environment variables
    num_books_to_process = 30

    kafka_client_config = {"bootstrap.servers": "localhost:29092,localhost:29093"}
    html_request_timeout = 5
    topic = "important-topic"
    title_regex_prefix = "^Title:"
    filename_glob_pattern = "*.txt"
    base_url = "http://www.gutenberg.org/robot/"
    starting_uri = "harvest?filetypes[]=txt&langs[]=en"
    list_of_sending_stats = []
    list_of_failed_to_process_books = []

    archive_links_list = retrieve_archive_links(base_url, starting_uri, num_books_to_process, html_request_timeout)
    if not archive_links_list:
        print("ERR: Unable to retrieve any links. Exiting")
        sys.exit(3)

    # Download and un-archive files
    if len(archive_links_list) > 0:
        print("{} links retrieved".format(len(archive_links_list)))
        print("Configured to process {} archive(s)".format(num_books_to_process))

        # Iterate over the requested number of archives
        for url in archive_links_list[:num_books_to_process]:
            print("\n> Processing archive: {}".format(url))

            # todo: add error handling & optional sleep period
            # Steam the download to temporary file
            r = requests.get(url, stream=True, timeout=html_request_timeout)

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
                        result = process_book_in_full(client=p, file_path=list_of_txt_files[0],
                                                      prefix=title_regex_prefix, kafka_topic=topic)

                        # Successful book title lookup and messages have been sent to kafka
                        if result:
                            print("Waiting until all the messages have been delivered to the broker...")
                            p.flush()

                            # Add to successful list to allow us to display stats at the end
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
        display_producer_stats(list_of_sending_stats, list_of_failed_to_process_books)

        print("\nComplete!")

    else:
        print("ERR: No links were returned. Exiting")
        sys.exit(2)
