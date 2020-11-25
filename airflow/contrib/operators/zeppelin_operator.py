from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.zeppelin_hook import ZeppelinHook


class ZeppelinOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id,
                 note_id,
                 *args,
                 **kwargs):
        super(ZeppelinOperator, self).__init__(*args, **kwargs)
        self.note_id = note_id
        self.z_hook = ZeppelinHook.get_hook(conn_id)

    def execute(self, context):
        self.z_hook.run_note(self.note_id)
