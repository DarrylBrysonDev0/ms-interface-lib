import os
import traceback
import time
from datetime import datetime
import uuid
import sys
import getopt

from microsrv_interface.comm_interface import *

def set_env_param(paramName,defaultStr):
    param = os.getenv(paramName)
    res = defaultStr if not param else param
    return res

def file_name_publisher():
    # Set sftp interface
    sftp_interface = sftp_CONN()
    # Set queue interface
    rbt_interface = queue_CONN()

    source_directory = set_env_param('SOURCE_PATH','/src')
    pub_cnt = 0
    try:
        # Connect to SFTP server
        print(' [*] Connecting to SFTP server')
        with sftp_interface as sftp:
            print(' [+] Connected to SFTP server')
            # Connect to RabbitMQ server
            print(' [*] Connecting to RabbitMQ server')
            with rbt_interface as rbt_params:
                print(' [+] Connected to RabbitMQ')
                # List files in dir
                print(' [*] Pulling file list')
                file_list = sftp_interface.get_dir_list(sftp, source_directory)
                print(' [+] Files found: {0}'.format(str(len(file_list))))
                # For each file name publish as message to output
                for file_name in file_list:
                    rbt_interface.write_output(file_name)
                    pub_cnt+=1
                    if pub_cnt>= rbt_interface.pub_limit:
                        break
                print(' [+] File paths published: {0}'.format(pub_cnt))
    except Exception as err:
        print()
        print("An error occurred while publishing file names to queues")
        print(str(err))
        traceback.print_tb(err.__traceback__)
    return
def file_name_consumer():
    # Set queue interface
    rbt_interface = queue_CONN()
    try:
        # Connect to RabbitMQ server
        print(' [*] Connecting to RabbitMQ server')
        with rbt_interface as rbt_params:
            print(' [+] Connected to RabbitMQ')
            # List files in dir
            def input_callback(ch, method, properties, msg):
                try:
                    print(msg)
                    # Ack message proc completion
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as err:
                    print()
                    print("An error occurred while retrieving message.")
                    print(str(err))
                    traceback.print_tb(err.__traceback__)
            rbt_interface.set_input_function(input_callback)
            rbt_interface.start_input_stream()
        time.sleep(frq)
    except Exception as err:
        print()
        print("An error occurred while publishing file names to queues")
        print(str(err))
        traceback.print_tb(err.__traceback__)
    return

def main(argv):
    try:
        opts, args = getopt.getopt(argv,"t:",["commtype=","type="])
    except getopt.GetoptError:
        print ('<app>.py -t <commtype>')
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-t", "--commtype","--type"):
            if arg == 'publish':
                file_name_publisher()
            elif arg == 'consume':
                file_name_consumer()
    return

if __name__ == '__main__':
    try:
        # main()
        main(sys.argv[1:])
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)