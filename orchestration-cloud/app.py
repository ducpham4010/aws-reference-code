from chalice import Chalice

from chalicelib.orchestration.blueprints import orcs_handlers
app = Chalice(app_name='orchestration-cloud')
app.api.binary_types.append("*/*")

app.register_blueprint(orcs_handlers)