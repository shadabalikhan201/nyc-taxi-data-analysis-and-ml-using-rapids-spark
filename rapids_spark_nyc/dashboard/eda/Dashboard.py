import sys
import pandas as pd
from rapids_spark_nyc.dashboard.DashApp import DashApp
from rapids_spark_nyc.read.Reader import Reader
from rapids_spark_nyc.spark_session.Spark import Spark
import dash
from dash.dash_table import DataTable
from loguru import logger
from dash import dash_table, html, dcc, Output, Input, State
import dash_bootstrap_components as dbc


class Dashboard:
    app = DashApp.get_dash_app()

    global_batch_size = 100
    global_page_size = 25
    global_batch_number = 0

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
                        children=[
                            dcc.Store(id='yellow_tripdata_jan_df',
                                      data=Dashboard.__get_df_next_batch('yellow_tripdata.jan', 0,
                                                                         Dashboard.global_batch_size).to_json(orient='split'),
                                      clear_data=False,
                                      storage_type='session'
                                      ),
                            dcc.Store(id='page_before_transition',
                                      data=-1,
                                      clear_data=False,
                                      storage_type='session'
                                      ),
                            self.__get_individual_dataset_tables('yellow_tripdata_jan', linked_dashboards),
                        ],
                        id="nyc_dt_collapse",
                        is_open=True,
                    )
                ]
            )
        ])
        logger.info('returning from Dashboard class homepage_layout() method')
        return home_layout

    @app.callback(Output('yellow_tripdata_jan', 'data'),
                  Output('yellow_tripdata_jan_df', "data"),
                  Output('page_before_transition', "data"),
                  Input('yellow_tripdata_jan', "page_current"),
                  State('yellow_tripdata_jan', "page_size"),
                  State('yellow_tripdata_jan_df', "data"),
                  State('page_before_transition', "data"))
    def update_nyc_dt(page_current, page_size, data, page_before_transition):
        logger.info('start of Dashboard class update_nyc_dt() callback method ==== : page number: ' + str(page_current))
        batch_index = page_current % (Dashboard.global_batch_size // Dashboard.global_page_size)
        page_transition_type = ''
        if (page_current - page_before_transition) > 0:# page transition towards next page ( lower to higher; eg: 2 to 3 ); page_transition_type = 'next'
            logger.info('Inside Dashboard class update_nyc_dt() callback method for next case')
            if batch_index == 0:# time to reload the df batch for next case
                if page_current != 0:
                    Dashboard.global_batch_number = Dashboard.global_batch_number + 1
                    logger.info('Inside if block of if block of Dashboard class update_nyc_dt() callback method')
                    batch_df = Dashboard.__get_df_next_batch('yellow_tripdata.jan',
                                                             Dashboard.global_batch_number * Dashboard.global_batch_size,
                                                             (Dashboard.global_batch_number + 1) * Dashboard.global_batch_size)
                    df = batch_df.iloc[
                         batch_index * Dashboard.global_page_size: (batch_index + 1) * Dashboard.global_page_size]
                else:
                    logger.info('Inside else block of if block of Dashboard class update_nyc_dt() callback method')
                    batch_df = pd.read_json(data, orient='split')
                    # df = batch_df.iloc[(page_current - 1) * page_size: page_current * page_size]
                    df = batch_df.iloc[batch_index * page_size: (batch_index + 1) * page_size]
            else:
                logger.info('Inside else block of Dashboard class update_nyc_dt() callback method')
                batch_df = pd.read_json(data, orient='split')
                # df = batch_df.iloc[(page_current - 1) * page_size: page_current * page_size]
                df = batch_df.iloc[
                     batch_index * Dashboard.global_page_size: (batch_index + 1) * Dashboard.global_page_size]

        else:# page transition towards previous page (higher to lower; eg: 3 to 2); page_transition_type = 'previous'
            logger.info('Inside Dashboard class update_nyc_dt() callback method for previous case')

            if batch_index == 3:# time to reload the df batch for previous case
                Dashboard.global_batch_number = Dashboard.global_batch_number - 1
                logger.info('Inside if block of Dashboard class update_nyc_dt() callback method for previous case')
                batch_df = Dashboard.__get_df_next_batch('yellow_tripdata.jan',
                                                         Dashboard.global_batch_number * Dashboard.global_batch_size,
                                                         (
                                                                 Dashboard.global_batch_number + 1) * Dashboard.global_batch_size)
                df = batch_df.iloc[
                     batch_index * Dashboard.global_page_size: (batch_index + 1) * Dashboard.global_page_size]
            else:
                logger.info('Inside else block of Dashboard class update_nyc_dt() callback method for previous case')
                batch_df = pd.read_json(data, orient='split')
                # df = batch_df.iloc[(page_current - 1) * page_size: page_current * page_size]
                df = batch_df.iloc[
                     batch_index * Dashboard.global_page_size: (batch_index + 1) * Dashboard.global_page_size]

        logger.info('returning from Dashboard class update_nyc_dt() callback method')
        return df.to_dict('records'), batch_df.to_json(orient='split'), page_current

    @app.callback(Output(component_id='nyc_dt_collapse', component_property='is_open'),
                  [Input(component_id='nyc_dt_button', component_property='n_clicks'),
                   Input(component_id='nyc_dt_collapse', component_property='is_open'), ])
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

    # @staticmethod
    def __get_individual_dataset_tables(self, dataset_id: str, linked_dashboards: list[list[str]]) -> DataTable:
        logger.info('start of Dashboard class __get_individual_dataset_tables() method')

        df_table = None
        for dashboard_data in linked_dashboards:
            id1 = dashboard_data[0]

            if id1 == dataset_id:
                current_page = 0 # current_page or page_current is always 1 less than page number.
                batch_index = current_page % (Dashboard.global_batch_size // Dashboard.global_page_size)
                df = Dashboard.__get_df_next_batch('yellow_tripdata.jan', 0, Dashboard.global_page_size) \
                         .iloc[batch_index * Dashboard.global_page_size: (batch_index+1) * Dashboard.global_page_size]
                df_table = dash_table.DataTable(id=dataset_id,
                                                data=df.to_dict('records'),
                                                columns=[{"name": i, "id": i} for i in df.columns],
                                                page_current=0,
                                                page_size=Dashboard.global_page_size,
                                                #page_count=Dashboard.__get_df_for_datatable('yellow_tripdata.jan').count()//page_size,
                                                page_action='custom')
                break
        logger.info('returning from Dashboard class __get_individual_dataset_tables() method')
        return df_table

    @staticmethod
    def __get_df_for_datatable(delta_table_nm: str) -> pd.DataFrame:
        logger.info('start of Dashboard class __get_df_for_datatable() method')
        project_home = sys.argv[1]
        spark_session = Spark.get_spark_session(project_home)
        df = Reader().read_versioned(spark_session, 'delta', True, delta_table_nm).toPandas()
        logger.info('returning from Dashboard class __get_df_for_datatable() method')
        return df.iloc[0: 155]

    @staticmethod
    def __get_df_next_batch(delta_table_nm: str, start_index: int, end_index: int) -> pd.DataFrame:
        logger.info('start of Dashboard class __get_df_next_batch() method')
        df = Dashboard.__get_df_for_datatable(delta_table_nm)
        df = df.iloc[start_index: end_index]
        logger.info('returning from Dashboard class __get_df_next_batch() method')
        return df