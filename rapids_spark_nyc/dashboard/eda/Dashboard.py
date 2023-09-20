from rapids_spark_nyc.dashboard.DashApp import DashApp
from rapids_spark_nyc.utilities.DashboardUtil import DashboardUtil
from rapids_spark_nyc.spark_session.Spark import Spark
import dash
from dash import dash_table, html, dcc, Output, Input
import dash_bootstrap_components as dbc


class Dashboard:

    app = DashApp.get_dash_app()

    def __init__(self, biospark_project_home):
        self.biospark_project_home = biospark_project_home

    def get_dashboard_home(self, id: str, linked_dashboards: list[list[str]]):
        self.app.layout = self.homepage_layout(id, linked_dashboards)
        self.app.run_server(debug=False)

    @DashboardUtil.returns_dash_div
    def homepage_layout(self, id: str, linked_dashboards: list[list[str]]):
        return html.Div(id=id, children=[
            html.H1(id, id='dashboard_title'),
            html.Div(
                id='dashboard1',
                children=[
                    html.H3('dashboard1', id='title_dashboard1'),
                    html.Button('fastq_sequence_dashboard', id='fastq_sequence_dashboard', n_clicks=0),
                    dbc.Collapse(
                        self.__get_individual_dataset_tables('fastq_sequence', linked_dashboards),
                        id="fastq_sequence_dashboard-collapse",
                        is_open=False,
                    )
                ]
            ),
            html.Div(
                id='dashboard2',
                children=[
                    html.H3('dashboard2', id='title_dashboard2'),
                    dcc.Store(id="linked_dashboards_store2", data=linked_dashboards),
                    html.Button('nucleotide_count_dashboard', id='nucleotide_count_dashboard', n_clicks=0),
                    dbc.Collapse(
                        self.__get_individual_dataset_tables('nucleotide_count', linked_dashboards),
                        id="nucleotide_count_dashboard-collapse",
                        is_open=False,
                    )
                ]
            )

        ])

    @app.callback(Output(component_id='fastq_sequence_dashboard-collapse', component_property='is_open'),
                  [Input(component_id='fastq_sequence_dashboard', component_property='n_clicks'),
                   Input(component_id='fastq_sequence_dashboard-collapse', component_property='is_open')])
    def hide_fastq_sequence_dataset_table(n_clicks, is_open):
        ctx = dash.callback_context
        show = None
        if ctx.triggered:
            button_clicked_id = ctx.triggered[0]['prop_id'].split('.')[0]
            if button_clicked_id=='fastq_sequence_dashboard':
                if  n_clicks:
                    show = not is_open
        return show

    @app.callback(Output(component_id='nucleotide_count_dashboard-collapse', component_property='is_open'),
                  [Input(component_id='nucleotide_count_dashboard', component_property='n_clicks'),
                   Input(component_id='nucleotide_count_dashboard-collapse', component_property='is_open')])
    def hide_fastq_sequence_dataset_table(n_clicks, is_open):
        ctx = dash.callback_context
        show = None
        if ctx.triggered:
            button_clicked_id = ctx.triggered[0]['prop_id'].split('.')[0]
            if button_clicked_id == 'nucleotide_count_dashboard':
                if n_clicks:
                    show = not is_open
        return show

    @DashboardUtil.returns_dash_datatable
    def __get_individual_dataset_tables(self, dataset_id: str, linked_dashboards: list[list[str]]):
        df_table = None
        glow_spark_session = Spark.get_spark_session(self.biospark_project_home)
        for dashboard_data in linked_dashboards:
            id1 = dashboard_data[0]
            df = glow_spark_session.sql('select * from {}'.format(dashboard_data[1]))
            if id1 == dataset_id:
                df_table = dash_table.DataTable(id=id1, data=df.toPandas().to_dict('records'),
                                                columns=[{"name": i, "id": i} for i in df.toPandas().columns],
                                                page_action='native', page_size=25)
                break
        return df_table
