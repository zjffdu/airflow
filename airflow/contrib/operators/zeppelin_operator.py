from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.zeppelin_hook import ZeppelinHook


class ZeppelinOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id,
                 note_id,
                 interpreter,
                 intp_properties,
                 code,
                 *args,
                 **kwargs):
        super(ZeppelinOperator, self).__init__(*args, **kwargs)
        self.note_id = note_id
        self.interpreter = interpreter
        self.intp_properties = intp_properties
        self.code = code
        self.z_hook = ZeppelinHook.get_hook(conn_id)

    def execute(self, context):
        if self.note_id:
            self.z_hook.run_note(self.note_id)
        else:
            if not self.interpreter:
                raise Exception('interpreter is not specified')
            if not self.code:
                raise Exception('code is not specified')
            self.z_hook.run_code(self.interpreter, self.code, self.intp_properties)
