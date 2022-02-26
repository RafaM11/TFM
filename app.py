import dash

app = dash.Dash(__name__)
app.title = 'NYC Traffic'
app.config.suppress_callback_exceptions = True
app.scripts.config.serve_locally = True