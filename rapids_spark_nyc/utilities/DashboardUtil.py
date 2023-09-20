from dash import Dash
from dash.dash_table import DataTable
from dash.html import Div


class DashboardUtil:

    @staticmethod
    def returns_dash_app(func):
        def dash_app_wrapper(*args):
            result = func(*args)
            assert type(result) == Dash
            return result

        return dash_app_wrapper

    @staticmethod
    def returns_dash_datatable(func):
        def dash_datatable_wrapper(*args):
            result = func(*args)
            assert type(result) == DataTable
            return result

        return dash_datatable_wrapper

    @staticmethod
    def returns_dash_div(func):
        def dash_div_wrapper(*args):
            result = func(*args)
            assert type(result) == Div
            return result

        return dash_div_wrapper
