import flask
from dash import Dash

from rapids_spark_nyc.utilities.DashboardUtil import DashboardUtil


class DashApp:
    __app = None

    @staticmethod
    def __init_dash_app():
        external_stylesheets = ['https://bootswatch.com/5/sketchy/bootstrap.min.css']
        flaskserver = flask.Flask(__name__)
        title = "Bio Nome Dashboard"
        if DashApp.__app is None:
            DashApp.__app = Dash(name=__name__, title=title, prevent_initial_callbacks=True, suppress_callback_exceptions=True, server=flaskserver, external_stylesheets=external_stylesheets)

    @staticmethod
    @DashboardUtil.returns_dash_app
    def get_dash_app():
        DashApp.__init_dash_app()
        return DashApp.__app
