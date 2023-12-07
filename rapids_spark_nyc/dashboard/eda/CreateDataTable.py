import sys
import dash
import pandas as pd
from dash import html, Output, Input, State, dash_table, dcc
from dash.dash_table import DataTable
import dash_bootstrap_components as dbc
from loguru import logger

from rapids_spark_nyc.dashboard.DashApp import DashApp
from rapids_spark_nyc.read.Reader import Reader
from rapids_spark_nyc.spark_session.Spark import Spark


class DataTableDashboard:
    """
    DataTableDashboard is a class that encapsulates the functionality related to the table visualization in a Dash application.

    The class provides the capability to retrieve and display data using Dash DataTable and include some data navigation functionality
    like paginating large datasets and batch loading of data to optimize performance.

    It also provides controls to hide/unhide the data table. The data for the visualization is read from a delta table.

    The app instance is retrieved from the `app` attribute.

    Attributes
    ----------
    app : dash.Dash
        The singleton instance of the Dash application.
    global_batch_size : int
        The size for each batch retrieved.
    global_page_size : int
        The size for each page in datatable.
    global_batch_number : int
        The number of the current batch.

    Methods
    -------
    __init__():
        The constructor of the class.

    datatable_layout(id: str, linked_dashboards: list[list[str]]) -> html.Div:
        Returns the layout for the DataTableDashboard.

    update_nyc_dt(page_current, page_size, data, page_before_transition):
        Dash callback function to update datatable data.

    hide_nyc_yellow_tripdata_table(n_clicks, is_open):
        Dash callback function to collapse or expand datatable.

    __get_individual_dataset_tables(self, dataset_id: str, linked_dashboards: list[list[str]]) -> DataTable:
        Returns a DataTable corresponding to the first batch of the specific dataset associated with a linked dashboard.
    __get_df_for_datatable(delta_table_nm: str) -> pd.DataFrame:
        Retrieves data from a delta table and converts it to a Pandas DataFrame.
    __get_df_next_batch(delta_table_nm: str, start_index: int, end_index: int) -> pd.DataFrame:
        Retrieves a batch of data from a delta table and converts it to a pandas DataFrame.
    """
    app = DashApp.get_dash_app()

    def __init__(self):
        pass

    global_batch_size = 100
    global_page_size = 25
    global_batch_number = 0

    def datatable_layout(self, id: str, linked_dashboards: list[list[str]]) -> html.Div:
        """
        Provides the layout for the DataTableDashboard.

        This method constructs the layout for the Dashboard including a H1 title, a button for show/hide functionality,
        a div to hold the data table, and the data table itself. It also includes stores to keep track of pagination
        and batching related information. Initial batch of data is loaded during the creation of layout.

        Parameters
        ----------
        id : str
            The id to assign to the main Div and H1 elements.

        linked_dashboards : list[list[str]]
            The list of linked dashboards, formatted as list of lists, each inner list containing the id of the linked dashboard.

        Returns
        -------
        dash_html_components.Div
            A Div containing the layout for the DataTableDashboard.

        Notes
        -----
        Any changes to the layout should correspondingly be handled in the callback functions.
        """
        logger.info('start of Dashboard class homepage_layout() method')

        layout = html.Div(id=id, children=[
            html.H1(id, id='nyc_taxidata_title'),
            html.Div(
                id='nyc_dt',
                children=[
                    html.H3('NYC Data Table', id='nyc_dt_title'),
                    html.Button('Show/Hide NYC Data Table', id='nyc_dt_button', n_clicks=0),
                    dbc.Collapse(
                        children=[
                            dcc.Store(id='yellow_tripdata_jan_df',
                                      data=DataTableDashboard.__get_df_next_batch('yellow_tripdata.jan', 0,
                                                                                  DataTableDashboard.global_batch_size).to_json(
                                          orient='split'),
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
        return layout

    @app.callback(Output('yellow_tripdata_jan', 'data'),
                  Output('yellow_tripdata_jan_df', "data"),
                  Output('page_before_transition', "data"),
                  Input('yellow_tripdata_jan', "page_current"),
                  State('yellow_tripdata_jan', "page_size"),
                  State('yellow_tripdata_jan_df', "data"),
                  State('page_before_transition', "data"))
    def update_nyc_dt(page_current, page_size, data, page_before_transition):
        """
        Callback to update data in the Dash datatable based on page navigation.

        This method decides whether user is navigating forwards or backwards based on the page_current and
        page_before_transition states. Based on that navigation, it prepares the next set of data to be
        displayed on the datatable, and updates the page_before_transition and batch dataframe (yellow_tripdata_jan_df).

        The handling of pagination is divided into a unique batch and page indexing, to allow batch loading of data.

        Parameters
        ----------
        page_current : int
            Current page on the datatable.
        page_size : int
            Number of rows per page on the datatable.
        data : str
            Current batch data in a json form.
        page_before_transition : int
            The page index before the user switched to the current page.

        Returns
        -------
        List
            Returns an updated data for datatable, updated batch data in a json form and the current page index.

        Notes
        -----
        Batch here refers to batches of data retrieved in one go, to be served across multiple pages.
        """
        logger.info('start of Dashboard class update_nyc_dt() callback method ==== : page number: ' + str(page_current))
        batch_index = page_current % (DataTableDashboard.global_batch_size // DataTableDashboard.global_page_size)
        page_transition_type = ''
        if (
                page_current - page_before_transition) > 0:  # page transition towards next page ( lower to higher; eg: 2 to 3 ); page_transition_type = 'next'
            logger.info('Inside Dashboard class update_nyc_dt() callback method for next case')
            if batch_index == 0:  # time to reload the df batch for next case
                if page_current != 0:
                    DataTableDashboard.global_batch_number = DataTableDashboard.global_batch_number + 1
                    logger.info('Inside if block of if block of Dashboard class update_nyc_dt() callback method')
                    batch_df = DataTableDashboard.__get_df_next_batch('yellow_tripdata.jan',
                                                                      DataTableDashboard.global_batch_number * DataTableDashboard.global_batch_size,
                                                                      (
                                                                              DataTableDashboard.global_batch_number + 1) * DataTableDashboard.global_batch_size)
                    df = batch_df.iloc[
                         batch_index * DataTableDashboard.global_page_size: (
                                                                                    batch_index + 1) * DataTableDashboard.global_page_size]
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
                     batch_index * DataTableDashboard.global_page_size: (
                                                                                batch_index + 1) * DataTableDashboard.global_page_size]

        else:  # page transition towards previous page (higher to lower; eg: 3 to 2); page_transition_type = 'previous'
            logger.info('Inside Dashboard class update_nyc_dt() callback method for previous case')

            if batch_index == 3:  # time to reload the df batch for previous case
                DataTableDashboard.global_batch_number = DataTableDashboard.global_batch_number - 1
                logger.info('Inside if block of Dashboard class update_nyc_dt() callback method for previous case')
                batch_df = DataTableDashboard.__get_df_next_batch('yellow_tripdata.jan',
                                                                  DataTableDashboard.global_batch_number * DataTableDashboard.global_batch_size,
                                                                  (
                                                                          DataTableDashboard.global_batch_number + 1) * DataTableDashboard.global_batch_size)
                df = batch_df.iloc[
                     batch_index * DataTableDashboard.global_page_size: (
                                                                                batch_index + 1) * DataTableDashboard.global_page_size]
            else:
                logger.info('Inside else block of Dashboard class update_nyc_dt() callback method for previous case')
                batch_df = pd.read_json(data, orient='split')
                # df = batch_df.iloc[(page_current - 1) * page_size: page_current * page_size]
                df = batch_df.iloc[
                     batch_index * DataTableDashboard.global_page_size: (
                                                                                batch_index + 1) * DataTableDashboard.global_page_size]

        logger.info('returning from Dashboard class update_nyc_dt() callback method')
        return df.to_dict('records'), batch_df.to_json(orient='split'), page_current

    @app.callback(Output(component_id='nyc_dt_collapse', component_property='is_open'),
                  [Input(component_id='nyc_dt_button', component_property='n_clicks'),
                   Input(component_id='nyc_dt_collapse', component_property='is_open'), ])
    def hide_nyc_yellow_tripdata_table(n_clicks, is_open):
        """
        Callback to handle the click event of the table show/hide button.

        This method changes the visibility status of the table (nyc_dt_collapse) every time its button (nyc_dt_button)
        is clicked. The visibility status is returned as a boolean value.

        Parameters
        ----------
        n_clicks : int
            Number of times the button (nyc_dt_button) was clicked.
        is_open : bool
            Current visibility status of the table (True if visible, False otherwise).

        Returns
        -------
        bool
            Updated visibility status of the table (Switches status every time this function is called).

        """
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
        """
        Returns a DataTable corresponding to the first batch of the specific dataset associated with a linked dashboard.

        This method iterates over the list of linked dashboards. It checks if the id of the dashboard matches the
        dataset_id argument. When they match it retrieves the first batch of data and creates a DataTable for that dataset.

        Parameters
        ----------
        dataset_id : str
            The id of the dataset for which a DataTable should be created.
        linked_dashboards: list[list[str]]
            The list of linked dashboards in the format: [[id, name], [id, name], ...].

        Returns
        -------
        dash_table.DataTable
            The DataTable for the specified dataset.

        Notes
        -----
        The DataTable returned contains only the first batch of the dataset. Pagination is controlled via callbacks.
        """
        logger.info('start of Dashboard class __get_individual_dataset_tables() method')

        df_table = None
        for dashboard_data in linked_dashboards:
            id1 = dashboard_data[0]

            if id1 == dataset_id:
                current_page = 0  # current_page or page_current is always 1 less than page number.
                batch_index = current_page % (
                        DataTableDashboard.global_batch_size // DataTableDashboard.global_page_size)
                df = DataTableDashboard.__get_df_next_batch('yellow_tripdata.jan', 0,
                                                            DataTableDashboard.global_page_size) \
                         .iloc[
                     batch_index * DataTableDashboard.global_page_size: (
                                                                                batch_index + 1) * DataTableDashboard.global_page_size]
                df_table = dash_table.DataTable(id=dataset_id,
                                                data=df.to_dict('records'),
                                                columns=[{"name": i, "id": i} for i in df.columns],
                                                page_current=0,
                                                page_size=DataTableDashboard.global_page_size,
                                                # page_count=DataTableDashboard.__get_df_for_datatable('yellow_tripdata.jan').count()//page_size,
                                                page_action='custom')
                break
        logger.info('returning from Dashboard class __get_individual_dataset_tables() method')
        return df_table

    @staticmethod
    def __get_df_for_datatable(delta_table_nm: str) -> pd.DataFrame:
        """
        Retrieves data from a delta table and converts it to a Pandas DataFrame.

        This method reads a delta table given its name 'delta_table_nm', and converts it to a Pandas DataFrame.

        The Spark session used for this is created using the project home path found in sys.argv[1].

        Parameters
        ----------
        delta_table_nm : str
            The name of the delta table from which data is to be retrieved.

        Returns
        -------
        pandas.DataFrame
            A Pandas DataFrame containing the first 155 rows of the delta table.

        """
        logger.info('start of Dashboard class __get_df_for_datatable() method')
        project_home = sys.argv[1]
        spark_session = Spark.get_spark_session(project_home)
        df = Reader().read_versioned(spark_session, 'delta', True, delta_table_nm).toPandas()
        logger.info('returning from Dashboard class __get_df_for_datatable() method')
        # return df
        return df.iloc[0: 155]  # used for debugging

    @staticmethod
    def __get_df_next_batch(delta_table_nm: str, start_index: int, end_index: int) -> pd.DataFrame:
        """
        Retrieves a batch of data from a delta table and converts it to a pandas DataFrame.

        This method reads a delta table given its name 'delta_table_nm', converts it to a DataFrame and returns
        the specific batch of rows between 'start_index' and 'end_index'.

        Parameters
        ----------
        delta_table_nm : str
            The name of the delta table from which data is to be retrieved.
        start_index : int
            The starting index for the slice in the dataframe.
        end_index : int
            The ending index for the slice in the dataframe.

        Returns
        -------
        pandas.DataFrame
            A Pandas DataFrame containing the rows of the delta table between 'start_index' and 'end_index'.

        """
        logger.info('start of Dashboard class __get_df_next_batch() method')
        df = DataTableDashboard.__get_df_for_datatable(delta_table_nm)
        df = df.iloc[start_index: end_index]
        logger.info('returning from Dashboard class __get_df_next_batch() method')
        return df
