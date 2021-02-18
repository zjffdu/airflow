from airflow.operators.bash_operator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.zeppelin_hook import ZeppelinHook
from airflow.utils.operator_helpers import context_to_airflow_vars


class ZeppelinOperator(BaseOperator):

    template_fields = ('params', )

    @apply_defaults
    def __init__(self,
                 conn_id,
                 note_id,
                 params={},
                 *args,
                 **kwargs):
        super(ZeppelinOperator, self).__init__(*args, **kwargs)
        self.note_id = note_id
        self.params = params
        self.z_hook = ZeppelinHook.get_hook(conn_id)

    def execute(self, context):
        params = self.params
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        params.update(airflow_context_vars)
        if self.note_id:
            self.z_hook.run_note(self.note_id, params)
        else:
            if not self.interpreter:
                raise Exception('interpreter is not specified')
            if not self.code:
                raise Exception('code is not specified')
            self.z_hook.run_code(self.interpreter, self.code, self.intp_properties)
