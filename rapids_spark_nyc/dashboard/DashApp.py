import flask
from dash import Dash
from loguru import logger


class DashApp:
    """
    DashApp class encapsulates operations related to the frontend Dash application.

    It manages the singleton instance of the Dash application and initializes it with certain configurations
    such as external stylesheets, title, and others. The initialization happens only once and the instance is reused.

    Attribute
    ---------
    __app : dash.Dash
        The singleton instance of the Dash application.

    Methods
    -------
    __init_dash_app() :
        A static private method that initializes the Dash application if it hasn't been initialized already.

    get_dash_app() -> Dash:
        A static public method that returns the singleton instance of the Dash application.
    """
    __app = None

    @staticmethod
    def __init_dash_app():
        """
        This static and private method initializes the Dash app if it hasn't been initialized already.

        The app is set up with a few configurations, including a title, prevention of initial
        callbacks, suppression of callback exceptions, and use of external stylesheets.

        Parameters
        ----------
        None

        Returns
        -------
        None

        Notes
        -----
        Be cautious while changing configurations in the Dash app initialization,
        as they could potentially break the functionality of the application.
        """
        logger.info('start of DashApp class __init_dash_app() method')
        external_stylesheets = ['https://bootswatch.com/5/sketchy/bootstrap.min.css']
        flask_server = flask.Flask(__name__)
        title = "NYC Taxi Data Dashboard App"
        if DashApp.__app is None:
            DashApp.__app = Dash(name=__name__, title=title, prevent_initial_callbacks=True, suppress_callback_exceptions=True, server=flask_server, external_stylesheets=external_stylesheets)
        logger.info('returning from DashApp class __init_dash_app() method')

    @staticmethod
    def get_dash_app() -> Dash:
        """
        This static and public method returns an instance of the Dash application.

        This method ensures the Dash app is initialized by calling the `__init_dash_app` method
        and then returns the instance of the Dash app stored in the `__app` class-level variable.

        Parameters
        ----------
        None

        Returns
        -------
        Dash
            The instance of the Dash application.

        Notes
        -----
        This method should be used to get the same Dash app instance everytime.
        """
        logger.info('start of DashApp class get_dash_app() method')
        DashApp.__init_dash_app()
        logger.info('returning from DashApp class get_dash_app() method')
        return DashApp.__app
