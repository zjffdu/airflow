from airflow.hooks.base_hook import BaseHook
from pyzeppelin import ClientConfig, ZeppelinClient, ZSession
import logging


class ZeppelinHook(BaseHook):
    
    def __init__(self, z_conn):
        self.z_conn = z_conn
        zeppelin_url = "http://" + self.z_conn.host + ":" + str(self.z_conn.port)
        self.client_config = ClientConfig(zeppelin_url)
        self.z_client = ZeppelinClient(self.client_config)
        # if z_conn.login and z_conn.password:
        #    self.z_client.login(z_conn.login, z_conn.password)

    @classmethod
    def get_hook(cls, conn_id='zeppelin_default'):
        z_conn = cls.get_connection(conn_id)
        return ZeppelinHook(z_conn=z_conn)

    def run_note(self, note_id, params = {}):
        note_result = self.z_client.execute_note(note_id, params)
        if not note_result.is_success():
            raise Exception("Fail to run note, note_result: {}".format(str(note_result)))
        else:
            logging.info("note {} is executed successfully".format(note_id))

    def run_code(self, interpreter, code, sub_interpreter = '', intp_properties = {}):
        z_session = ZSession(self.client_config, interpreter, intp_properties)
        result = z_session.execute(code, sub_interpreter)
        if result.is_success:
            logging.info('Run successfully')
        else:
            raise Exception('Fail to run code, execute_result: {}'.format(result))


