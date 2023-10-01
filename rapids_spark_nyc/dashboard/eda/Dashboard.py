import sys

from rapids_spark_nyc.dashboard.DashApp import DashApp
from rapids_spark_nyc.utilities.DashboardUtil import DashboardUtil
from rapids_spark_nyc.read.Reader import Reader
from rapids_spark_nyc.spark_session.Spark import Spark
import dash
from loguru import logger
from dash import dash_table, html, dcc, Output, Input
import dash_bootstrap_components as dbc


class Dashboard:
    app = DashApp.get_dash_app()

    def __init__(self):
        logger.info('Dashboard class object instantiation')
        pass

    def get_dashboard_home(self, id: str, linked_dashboards: list[list[str]]):
        logger.info('start of Dashboard class get_dashboard_home() method')
        self.app.layout = self.homepage_layout(id, linked_dashboards)
        self.app.run_server(debug=True)
        logger.info('returning from Dashboard class get_dashboard_home() method')

    @DashboardUtil.returns_dash_div
    def homepage_layout(self, id: str, linked_dashboards: list[list[str]]) -> html.Div:
        logger.info('start of Dashboard class homepage_layout() method')

        home_layout = html.Div(id=id, children=[
            html.H1(id, id='nyc_taxidata_title'),
            html.Div(
                id='nyc_dt',
                children=[
                    html.H3('NYC Data Table', id='nyc_dt_title'),
                    html.Button('Show/Hide NYC Data Table', id='nyc_dt_button', n_clicks=0),
                    dbc.Collapse(
                        self.__get_individual_dataset_tables('yellow_tripdata.jan', linked_dashboards),
                        id="nyc_dt_collapse",
                        is_open=False,
                    )
                ]
            )
        ])
        logger.info('returning from Dashboard class homepage_layout() method')
        return home_layout

    @app.callback(Output(component_id='nyc_dt_collapse', component_property='is_open'),
                  [Input(component_id='nyc_dt_button', component_property='n_clicks'),
                   Input(component_id='nyc_dt_collapse', component_property='is_open')])
    def hide_nyc_yellow_tripdata_table(n_clicks, is_open):
        logger.info('start of Dashboard class hide_nyc_yellow_tripdata_table() callback method')
        ctx = dash.callback_context
        show = None
        if ctx.triggered:
            button_clicked_id = ctx.triggered[0]['prop_id'].split('.')[0]
            if button_clicked_id == 'nyc_dt_button':
                if n_clicks:
                    show = not is_open
        logger.info('returning from Dashboard class hide_nyc_yellow_tripdata_table() callback method')
        return show

    # @DashboardUtil.returns_dash_datatable
    def __get_individual_dataset_tables(self, dataset_id: str, linked_dashboards: list[list[str]]):
        logger.info('start of Dashboard class __get_individual_dataset_tables() method')
        df_table = None
        spark = Spark.get_spark_session(sys.argv[1])
        for dashboard_data in linked_dashboards:
            id1 = dashboard_data[0]
            df = Reader().read_versioned(spark, 'delta', True, 'yellow_tripdata.jan')
            if id1 == dataset_id:
                df_table = dash_table.DataTable(id=id1, data=df.toPandas().to_dict('records'),
                                                columns=[{"name": i, "id": i} for i in df.toPandas().columns],
                                                page_action='native', page_size=25)
                break
        logger.info('returning from Dashboard class __get_individual_dataset_tables() method')
        return df_table
