import flask
from dash import Dash
from loguru import logger
from rapids_spark_nyc.utilities.DashboardUtil import DashboardUtil


class DashApp:
    __app = None

    @staticmethod
    def __init_dash_app():
        logger.info('start of DashApp class __init_dash_app() method')
        external_stylesheets = ['https://bootswatch.com/5/sketchy/bootstrap.min.css']
        flaskserver = flask.Flask(__name__)
        title = "NYC Taxi Data Dashboard App"
        if DashApp.__app is None:
            DashApp.__app = Dash(name=__name__, title=title, prevent_initial_callbacks=True, suppress_callback_exceptions=True, server=flaskserver, external_stylesheets=external_stylesheets)
        logger.info('returning from DashApp class __init_dash_app() method')

    @staticmethod
    @DashboardUtil.returns_dash_app
    def get_dash_app():
        logger.info('start of DashApp class get_dash_app() method')
        DashApp.__init_dash_app()
        logger.info('returning from DashApp class get_dash_app() method')
        return DashApp.__app
