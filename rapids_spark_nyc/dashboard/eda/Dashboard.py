import sys
import pyspark.pandas as ps
from rapids_spark_nyc.dashboard.DashApp import DashApp
from rapids_spark_nyc.read.Reader import Reader
from rapids_spark_nyc.spark_session.Spark import Spark
import dash
from dash.dash_table import DataTable
from loguru import logger
from dash import dash_table, html, dcc, Output, Input
import dash_bootstrap_components as dbc


class Dashboard:
    app = DashApp.get_dash_app()

    def __init__(self):
        logger.info('Dashboard class object instantiation')

    def get_dashboard_home(self, id: str, linked_dashboards: list[list[str]]):
        logger.info('start of Dashboard class get_dashboard_home() method')
        self.app.layout = self.homepage_layout(id, linked_dashboards)
        self.app.run_server(debug=False)
        logger.info('returning from Dashboard class get_dashboard_home() method')

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
                        self.__get_individual_dataset_tables('yellow_tripdata_jan', linked_dashboards),
                        id="nyc_dt_collapse",
                        is_open=True,
                    )
                ]
            )
        ])
        logger.info('returning from Dashboard class homepage_layout() method')
        return home_layout

    @app.callback(Output('yellow_tripdata_jan', 'data'),
                  [Input('yellow_tripdata_jan', "page_current"),
                   Input('yellow_tripdata_jan', "page_size")])
    def update_nyc_dt(page_current, page_size):
        logger.info('start of Dashboard class update_nyc_dt() callback method')

        project_home = sys.argv[1]
        spark_session = Spark.get_spark_session(project_home)
        df = Reader().read_versioned(spark_session, 'delta', True, 'yellow_tripdata.jan')
        ps_df = ps.DataFrame(df).iloc[(page_current) * page_size:(page_current + 1) * page_size]
        pd_df = ps_df.to_pandas()
        df = None
        ps_df = None
        logger.info('returning from Dashboard class update_nyc_dt() callback method')

        return pd_df.to_dict('records')

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

    def __get_individual_dataset_tables(self, dataset_id: str, linked_dashboards: list[list[str]]) -> DataTable:
        logger.info('start of Dashboard class __get_individual_dataset_tables() method')
        project_home = sys.argv[1]
        df_table = None
        for dashboard_data in linked_dashboards:
            id1 = dashboard_data[0]
            if id1 == dataset_id:
                spark_session = Spark.get_spark_session(project_home)
                df = Reader().read_versioned(spark_session, 'delta', True, 'yellow_tripdata.jan')

                current_page = 1
                page_size = 25

                ps_df = ps.DataFrame(df).iloc[(current_page - 1) * page_size: current_page * page_size]
                df = None
                pd_df = ps_df.to_pandas()
                df_table = dash_table.DataTable(id=dataset_id,
                                                data=pd_df.to_dict('records'),
                                                columns=[{"name": i, "id": i} for i in pd_df.columns],
                                                page_current=current_page - 1,
                                                page_size=page_size,
                                                page_action='custom')
                break
        logger.info('returning from Dashboard class __get_individual_dataset_tables() method')
        return df_table
