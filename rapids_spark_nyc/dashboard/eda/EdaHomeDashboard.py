from rapids_spark_nyc.dashboard.DashApp import DashApp
from rapids_spark_nyc.dashboard.eda.CreateDataTable import DataTableDashboard
from loguru import logger


class EdaDashboard:
    """
    EdaDashboard class used for creating and managing an EDA dashboard using DashApp.

    Methods:
        - get_dashboard_home(): Gets the home layout of the dashboard with the given id and linked dashboards.

    Attributes:
        - app: A DashApp object representing the dashboard application.

    """
    app = DashApp.get_dash_app()

    def __init__(self):
        logger.info('Dashboard class object instantiation')

    def get_dashboard_home(self, id: str, linked_dashboards: list[list[str]]):
        """
        This method sets up the layout of the dashboard and runs the server. It does not return any value.

        :param id: The ID of the dashboard.
        :param linked_dashboards: A list of lists of strings, representing the linked dashboards.
        :return: None

        """
        logger.info('start of Dashboard class get_dashboard_home() method')
        self.app.layout = DataTableDashboard().datatable_layout(id, linked_dashboards)
        self.app.run_server(debug=False)
        logger.info('returning from Dashboard class get_dashboard_home() method')
