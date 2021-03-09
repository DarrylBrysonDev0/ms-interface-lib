import os
import pysftp
import traceback
import time
from datetime import datetime
import pika
import pathlib
import uuid
import sys
import pyodbc
import pandas as pd

##### Class #####
class sftp_CONN:
    # Ini
    def __init__(self):
        self.ResultAr = []

        self.remote_host = None
        self.port = None
        self.usr = None
        self.pwd = None
        self.src = None
        self.destPath = None

        self.conn = None

        return
    def __enter__(self):
        res = self.setup()
        pAr = self.to_list()
        return res
    def __exit__(self, type, value, traceback):
        self.close_conn()
        return
    def setup(self) -> None :
        self.from_env()
        conn = self.get_conn()
        return conn
    def from_env(self) -> None: #setAllParams(self):
        self.remote_host = self.set_env_param('SFTP_HOST',r'localhost')
        self.port = int(self.set_env_param('SFTP_PORT',r'22'))
        self.usr = self.set_env_param('SFTP_USR',r'admin')
        self.pwd = self.set_env_param('SFTP_PWD',r'pwd')
        self.src = self.set_env_param('SOURCE_PATH',r'/src')
        self.destPath = self.set_env_param('DEST_PATH',r'/trgt')
        c = pysftp.CnOpts()
        c.hostkeys = None
        self.cnopts = c
    def to_list(self):
        self.ResultAr.append(self.remote_host)
        self.ResultAr.append(self.port)
        self.ResultAr.append(self.usr)
        self.ResultAr.append(self.pwd)
        self.ResultAr.append(self.src)
        self.ResultAr.append(self.destPath)
        return self.ResultAr
    def set_env_param(self, paramName,defaultStr):
        param = os.getenv(paramName)
        res = defaultStr if not param else param
        return res
    def get_dir_list(self, scon, srcPath):
        # Set callback functions
        wtcb = pysftp.WTCallbacks()
        # Recursively map files
        scon.walktree(srcPath, fcallback=wtcb.file_cb, dcallback=wtcb.dir_cb, ucallback=wtcb.unk_cb)
        lAr = wtcb.flist
        return lAr
    def get_conn(self) -> pysftp.Connection:
        """
        Returns an SFTP connection object
        """
        if self.conn is None:
            cnopts = self.cnopts

            conn_params = {
                'host': self.remote_host,
                'port': self.port,
                'username': self.usr,
                'cnopts': cnopts
            }
            if self.pwd and self.pwd.strip():
                conn_params['password'] = self.pwd
            self.conn = pysftp.Connection(**conn_params)
        return self.conn
    def close_conn(self) -> None:
        """
        Closes the connection
        """
        if self.conn is not None:
            self.conn.close()
            self.conn = None
    def path_exists(self, path: str) -> bool:
        """
        Returns True if a remote entity exists

        :param path: full path to the remote file or directory
        :type path: str
        """
        conn = self.get_conn()
        return conn.exists(path)
    def create_directory(self, remote_directory: str) -> bool:
        """Change to this directory, recursively making new folders if needed.
        Returns True if any folders were created."""
        if self.conn is None:
            self.get_conn()
        sftp = self.conn

        if remote_directory == '/':
            # absolute path so change directory to root
            sftp.chdir('/')
            return
        if remote_directory == '':
            # top-level relative directory must exist
            return
        try:
            sftp.chdir(remote_directory) # sub-directory exists
        except IOError:
            dirname, basename = os.path.split(remote_directory.rstrip('/'))
            self.create_directory(dirname) # make parent directories
            sftp.mkdir(basename) # sub-directory missing, so created it
            sftp.chdir(basename)
            return True
    def download_file(self, locDir: str, remotePath: str) -> str:
        res = None
        if self.conn is None:
            self.get_conn()
        sftp = self.conn

        try:
            # Set local file paths
            rp = str(remotePath)
            p = pathlib.Path(rp.replace("b'", "").replace("'", ""))
            b = pathlib.Path(str(locDir))
            locPath = b.joinpath(p.name)
            # Copy files locally
            sftp.get(str(p), str(locPath))
            res = locPath
        except Exception as err:
            print()
            print("An error occurred while downloading file from SFTP server.")
            print(str(err))
            traceback.print_tb(err.__traceback__)
        return res
    def upload_file(self, locPath, remotePath) -> str:
        res = None
        if self.conn is None:
            self.get_conn()
        sftp = self.conn
        try:
            # Set local file paths
            rp = str(remotePath)
            locPath = str(locPath)
            rDir = pathlib.Path(rp).parent
            self.create_directory(rDir)
            # Copy files remotely
            sftp.put(locPath,rp)
            res = rp
        except Exception as err:
            print("An error occurred while uploading file to SFTP server.")
            print(str(err))
            traceback.print_tb(err.__traceback__)
            raise
        return res
    def delete_file(self, remotePath: str):
        if self.conn is None:
            self.get_conn()
        sftp = self.conn
        try:
            sftp.remove(remotePath)
        except Exception as err:
            print("An error occurred while deleting file from SFTP server.")
            print(str(err))
            traceback.print_tb(err.__traceback__)
    def append_file(self, remotePath_a, remotePath_b):
        if self.conn is None:
            self.get_conn()
        sftp = self.conn
        res = None
        try:
            # with sftp_conn.open(remotePath_a,'a') as f_a:
            with sftp.open(remotePath_b,'rb') as f_b:
                appStr = f_b.read().decode('utf-8') + '\n'
                remotePath_a.writelines(appStr)
            rp = str(remotePath_a)
            res = rp
        except Exception as err:
            print("An error occurred while appending text to file on SFTP server.")
            print(str(err))
            traceback.print_tb(err.__traceback__)
        return res

class queue_CONN:
    # Ini
    def __init__(self):
        # ini class attributes
        self.ResultAr = []

        self.rbt_srv = None
        self.queue_namespace = None
        self.src_queue = None
        self.dest_queue = None
        self.enable_namespace = None
        self.pub_limit = None

        self.in_conn = None
        self.out_conn = None
        # Create channels
        self.in_channel = None
        self.out_channel = None
        self.ns_channel = None

        # 
        self.message_cnt = 0
        return
    def __enter__(self):
        self.setup()
        pAr = self.to_list()
        return pAr
    def __exit__(self, type, value, traceback):
        self.close_all_connections()
        return
    def setup(self) -> None :
        self.from_env()
        self.create_named_channel_queues()
        return
    def _isAttribSet(self, attr) -> bool:
        res = False
        try: res =  hasattr(self,attr)
        except Exception as err: res = False
        return res
    def from_env(self) -> None: #setAllParams(self):
        '''
            Set class parameters from environment variables or set development defaults
        '''
        default_ns = str(uuid.uuid4().hex)
        self.rbt_srv = self.set_env_param('RABBIT_SRV',r'rabbit-queue')
        self.queue_namespace = self.set_env_param('NAMESPACE',default_ns)
        self.src_queue = self.set_env_param('INPUT_QUEUE',r'new_files')
        self.dest_queue = self.set_env_param('OUTPUT_QUEUE',r'processed_files')
        self.enable_namespace = bool(int(self.set_env_param('ENABLE_NAMESPACE_QUEUE',r'0')))
        self.pub_limit = int(self.set_env_param('PUBLISHING_LIMIT','20'))
        return
    def to_list(self):
        '''
            Return class parameters as a list
        '''
        self.ResultAr.append(self.rbt_srv)
        self.ResultAr.append(self.queue_namespace)
        self.ResultAr.append(self.src_queue)
        self.ResultAr.append(self.dest_queue)
        return self.ResultAr
    def set_env_param(self, paramName: str,defaultStr: str) -> str:
        param = os.getenv(paramName)
        res = defaultStr if not param else param
        return res
    def set_input_function(self, input_func) -> None:
        self._input_func = input_func
        return
    # Channel Managment
    ## Named channels
    ### Create channels
    def create_namespace_queues(self, chObj, nsStr: str) -> None:
        self.success_queue = 'pass_' + nsStr
        self.fail_queue = 'fail_' + nsStr
        self.progress_queue = 'status_' + nsStr
        chObj.queue_declare(queue=self.success_queue, durable=True)
        chObj.queue_declare(queue=self.fail_queue, durable=True)
        chObj.queue_declare(queue=self.progress_queue, durable=True)
        return
    def create_named_channel_queues(self) -> None:
        # Create connections
        self.in_conn = self.open_Connection()
        self.out_conn = self.open_Connection()
        # Create channels
        self.in_channel = self.open_channel(self.in_conn)
        self.out_channel = self.open_channel(self.out_conn)
        self.ns_channel = self.open_channel(self.out_conn)
        # Create queues
        self.in_channel.queue_declare(queue=self.src_queue, durable=True)
        self.out_channel.queue_declare(queue=self.dest_queue, durable=True)

        if self.enable_namespace:
            ## Create namespace based queues
            self.create_namespace_queues(self.ns_channel, self.queue_namespace)
        return
    ### Start/Stop Input channel
    def start_input_stream(self) -> None:
        print(' [*] Starting input stream')
        if (self.in_channel is not None) and (self._input_func is not None):
        # if self._isAttribSet(self.in_channel) and self._isAttribSet(self._input_func):
            self.ip_consuming_tag = self.start_consuming(self.in_channel, self.src_queue, self._input_func)
            print(' [+] Input stream started with consumer_tag {0}'.format(str(self.ip_consuming_tag)))
        return
    def stop_input_stream(self) -> None:
        if (self.in_channel is not None) and (self.ip_consuming_tag is not None):
        # if self._isAttribSet('in_channel') and self._isAttribSet('ip_consuming_tag'):
            self.stop_consuming(self.in_channel, self.ip_consuming_tag)
        return
    ### Write to Output channel
    def write_output(self, op_msg: str) -> None:
        # Build in publish limiter
        if (self.out_channel is not None) and (self.dest_queue is not None):
            self.publish_message(self.out_channel, self.dest_queue,op_msg)
        return
    ## General channels
    ### Publish message to queue
    def publish_message(self, channel, queueName: str, op_msg: str) -> None:
        try:
            # Build in publish limiter
            channel.basic_publish(exchange='',
                            routing_key=queueName,
                            body=op_msg)
        except Exception as err:
            print()
            print(" [!] Message not published.")
            print(str(err))
            traceback.print_tb(err.__traceback__)
        return
    ### Start/Stop consumer
    def start_consuming(self, channel, queueName, func):
        if (channel is not None) and (queueName is not None) and (func is not None):
            ctag = self.in_channel.basic_consume(self.src_queue, self._input_func)
            channel.start_consuming()
        return ctag
    def stop_consuming(self, ch, ch_tag) -> None:
        ch.basic_cancel(ch_tag)
        return
    # Connection State
    def open_Connection(self):
        connection = None
        try:
            connection =  (pika.BlockingConnection(
                                parameters=pika.ConnectionParameters(self.rbt_srv)))
        except Exception as err:
            print()
            print(" [!] Connection could not be established with queue server: {0}".format(self.rbt_srv))
            print(str(err))
            traceback.print_tb(err.__traceback__)
        return connection
    def open_channel(self, connObj):
        ch = None
        try:
            ch = connObj.channel()
            ch.basic_qos(prefetch_count=1)
        except Exception as err:
            print()
            print(" [!] Channel could not be created.")
            print(str(err))
            traceback.print_tb(err.__traceback__)
        return ch
    def close_all_connections(self) -> None:
        self.close_connection(self.in_conn)
        self.close_connection(self.out_conn)
        return
    def close_connection(self, conn) -> None:
        if conn is not None:
            if conn.is_open:
                conn.close()
        return
    # Communicate status
    ## Write Success
    def write_success(self, op_msg: str) -> None:
        # Build in publish limiter
        if (self.enable_namespace) and (self.out_channel is not None) and (self.success_queue is not None):
            self.publish_message(self.out_channel, self.success_queue,op_msg)
        return
    ## Write Fault
    def write_fault(self, op_msg: str) -> None:
        # Build in publish limiter
        if (self.enable_namespace) and (self.out_channel is not None) and (self.fail_queue is not None):
            self.publish_message(self.out_channel, self.fail_queue,op_msg)
        return
    ## Write Status
    def write_status(self, op_msg: str) -> None:
        # Build in publish limiter
        if (self.enable_namespace) and (self.out_channel is not None) and (self.progress_queue is not None):
            self.publish_message(self.out_channel, self.progress_queue, op_msg)
        return
    def iter_message_cnt() -> None:
        i = 0
        try:
            if self.message_cnt is not None:
                i = self.message_cnt
            i = i + 1
            self.message_cnt = i
        except Exception as err:
            print()
            print(' [!] Error proc iter_message_cnt...')
            print(str(err))
            traceback.print_tb(err.__traceback__)
        return

# Class for handling DB calls
class db_CONN:
    # Constructor
    def __init__ (self, configJson="ENV"):
        # Example usage
        # >>> import json
        # >>> cfg = json.load(open(jFile))
        # >>> dbcon = DBInterface(cfg)
        # Parameters
        self.dbConnection = None
        self.configData = None
        self._data_tbl = None
        self._db_tbl_name = None
        self._columns = None
        # self._subsel_col = None
        # Constaints
        self.WRITE_BUFFER_LIMIT = 500
        self.CONFIG_NODE = 'Source_DB'
        self.IS_VERBOSE = False
        # Ini connection parameters
        if str(configJson) == "ENV":
            self.from_env()
        else:
            self.configData = configJson[self.CONFIG_NODE]
    def __enter__(self):
        res = self.setup()
        # pAr = self.to_list()
        return res
    def __exit__(self, type, value, traceback):
        self.close_conn()
        return
    # Initializers 
    def setup(self) -> pyodbc.Connection:
        self.from_env()
        self.ConnectToDb()
        return self.dbConnection
    def from_env(self) -> None:
        res_dic = {}
        res_dic['ServerAddr']=self.set_env_param('SQL_SERVER_HOST',r'')
        res_dic['DBName']=self.set_env_param('DB_NAME',r'')
        res_dic['UserName']=self.set_env_param('DB_USER',r'')
        res_dic['Password']=self.set_env_param('DB_PASSWORD',r'')
        self.configData = res_dic
    # Supporting methods
    def to_list(self) -> list:
        res = list(self.configData.values())
        return res
    def set_env_param(self, paramName,defaultStr):
        param = os.getenv(paramName)
        res = defaultStr if not param else param
        return res
    
    # Dataframe handlers
    def set_df(self, df, db_tbl) -> None:
        self._data_tbl = df
        self._columns = self.get_df_columns()
        self._db_tbl_name = db_tbl
    def set_subselect_cols(self,col: list) -> None:
        # Set sub-select columns to override linked dataframe columns 
        if self._data_tbl is not None:
            # self._subsel_col = col
            self._data_tbl = self._data_tbl.reindex(columns=col)
    def get_df_columns(self) -> list:
        # Get current list of columns to use from linked dataframe
        res = []
        if self._data_tbl is not None:
            res = list(self._data_tbl.columns)
            # if self._subsel_col is not None: 
            #     res = self._subsel_col
            # else: 
            #     res = list(self._data_tbl.columns)
        return res
    def select_db_table(self, cnt=100):
        # Norm column list
        self._columns = self.get_df_columns()
        # Build sql select query from paired database table
        sel_str = 'SELECT TOP '+ str(cnt) + ' '
        sel_str += ', '.join('[{0}]'.format(c) for c in self._columns) + ' '
        # sel_str += ', '.join(('['+self._columns+']')) + ' '
        sel_str += 'FROM ' + self._db_tbl_name
        # Select to dataframe
        df = self.select_query(sel_str,self._columns)
        return df 
    def insert_dataframe(self) -> int:
        # Norm column list
        self._columns = self.get_df_columns()
        if self._data_tbl is not None:
            # Build insert query
            ins_str = 'INSERT INTO ' + self._db_tbl_name + ' '
            ins_str += '(' + ', '.join('[{0}]'.format(c) for c in self._columns) + ')'
            # Sub-select dataframe
            ins_df = self._data_tbl[self._columns]
            # default all fields to strings
            ins_df = ins_df.astype('string')
            # Write to db
            wrt_cnt = self.write_dataframe(ins_str, ins_df)
        return wrt_cnt
    # Connection managers
    def SetDBConfig(self, configJson):
        if self.IS_VERBOSE: print("Reading config")
        self.configData = configJson[self.CONFIG_NODE]
        return 0
    # method - Set connection
    def ConnectToDb (self):
        if self.dbConnection is None:
            connTime = StopWatch()
            
            conn = self.get_conn()
            self.dbConnection = conn 

            if self.IS_VERBOSE: print("Connection established in ",connTime.timeElapsed(),"sec.")
            connTime = None
        else:
            if self.IS_VERBOSE: print("Connection established")
        return 0
    # Return a db connection object
    def get_conn(self) -> pyodbc.Connection:
        cfg = self.configData
        server = cfg['ServerAddr']
        database = cfg['DBName'] 
        username = cfg['UserName'] 
        password = cfg['Password'] 
        
        if self.IS_VERBOSE: print("Connecting to database ",database)
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        return conn
    # Close connections
    def close_conn(self) -> None:
        if self.dbConnection is not None:
            self.dbConnection.close()
    # method - Select query
    def select_query (self, queryStr, fieldLs):
        # Example Use
        # >>> col = ['COMMENT_ID','REPAIR_NARRATIVE','CLAIM_NUMBER']
        # >>> sqlStr = '''SELECT TOP 100000
        # ...                 A.[CLM_COMTS_ID] AS [COMMENT_ID]
        # ...                 ,A.[COMT_DESC] AS [REPAIR_NARRATIVE]
        # ...                 ,A.[CLM_CD] AS [CLAIM_NUMBER]
        # ...             FROM [MST].[OWL_CLAIMS_COMMENTS_DDC_V] A
        # ...             LEFT OUTER JOIN (SELECT * FROM [ETL].[NARRATIVES_PROCESSED] WHERE [CSC_EXTRACTED] = 0) B
        # ...                 ON A.CLM_COMTS_ID = B.[COMMENT_ID]
        # ...             WHERE B.COMMENT_ID IS NULL'''
        # >>> dbcon.SelectQuery(sqlStr,col)

        queryTime = StopWatch()
        
        self.ConnectToDb()
        # Execute query and return results to a dataframe
        rslDF = pd.read_sql_query(queryStr, self.dbConnection )
        rslDF.columns= fieldLs #['COMMENT_ID','REPAIR_NARRATIVE','CLAIM_NUMBER']
        if self.IS_VERBOSE: print("Query executed in:",queryTime.timeElapsed())

        #return result set
        return rslDF
    # method - Insert query
    def write_dataframe (self, InsertQuery, sourceList):
        insertTime = StopWatch()
        # Establish a db connection
        self.ConnectToDb()
        conn = self.dbConnection

        # Set function scoped variables
        bufferSize = self.WRITE_BUFFER_LIMIT # Records to insert at a time
        rowCnt = 0 # Capture total number of records written
        i = 0 # Record count in current buffer
        sqlBase = InsertQuery # "INSERT INTO [OBJ].[NARRATIVE_CSC_REF] ([COMMENT_ID],[CLAIM_NUMBER],[EXTRACTED_CSC_REF]) VALUES"
        sqlBase += " VALUES"
        sqlStr = sqlBase

        
        # Generate cursor
        cursor = conn.cursor()
        # Iterate through rows and add to value list
        for idx,rowLs in sourceList.iterrows():
            if i == 0:
                sqlStr += "\n("
            else:
                sqlStr += "\n,("
            # 
            
            sepStr = ','
            valStr = sepStr.join('\'' + str(item) + '\'' for item in rowLs) #sepStr.join(rowLs)
            sqlStr += valStr+')'

            i+=1
            # Buffer row inserts 
            rowCnt+=1
            if (i >= bufferSize) or (rowCnt >= len(sourceList.index)):
                # Reset query and counter to base
                i = 0
                # Insert records to db 
                if self.IS_VERBOSE: 
                    if (i >= bufferSize): print("Buffer reached")
                    if (rowCnt >= len(sourceList.index)): print("EOD reached")
                    print("Row index:",rowCnt)
                try:
                    cursor.execute(sqlStr)
                except Exception as err:
                    print()
                    print("An error occurred while inserting records.")
                    print(str(err))
                    print()
                    print(sqlStr)
                    traceback.print_tb(err.__traceback__)
                conn.commit()
                sqlStr = sqlBase
        if self.IS_VERBOSE: print("Results written in:",insertTime.timeElapsed())
        # Return total number of inserts
        return rowCnt
# Class for automatic timer
class StopWatch:
    # Parameters
    t0 = None
    # Constructor
    def __init__ (self):
        self.t0 = time.time()
    # Methods
    def Reset (self):
        self.t0 = time.time()
    def Stop (self):
        self.t0 = None
    def timeElapsed(self):
        t1 = time.time()
        tE = t1 - self.t0
        return tE

##### Function #####
def set_env_param(paramName,defaultStr):
    param = os.getenv(paramName)
    res = defaultStr if not param else param
    return res
