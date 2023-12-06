import sys
import pandas as pd

from rapids_spark_nyc.dashboard.DashApp import DashApp
from rapids_spark_nyc.dashboard.eda.CreateDataTable import DataTableDashboard
from rapids_spark_nyc.read.Reader import Reader
from rapids_spark_nyc.spark_session.Spark import Spark
import dash
from dash.dash_table import DataTable
from loguru import logger
from dash import dash_table, html, dcc, Output, Input, State, Dash
import dash_bootstrap_components as dbc


class EdaDashboard:
    app = DashApp.get_dash_app()

    def __init__(self):
        logger.info('Dashboard class object instantiation')

    def get_dashboard_home(self, id: str, linked_dashboards: list[list[str]]):
        logger.info('start of Dashboard class get_dashboard_home() method')
        self.app.layout = DataTableDashboard().datatable_layout(id, linked_dashboards)
        self.app.run_server(debug=False)
        logger.info('returning from Dashboard class get_dashboard_home() method')

