import numpy as np
import plotly.graph_objs as go
import plotly.offline as offline_py

def spark_shape(df):
    """
    Returns a tuple representing the dimensionality of the Spark Dataframe
    
    Args:
    df pyspark.sql.dataframe.DataFrame: Return shape of this Spark dataframe
    
    Returns:
    shape tuple: Shape in tuple format (n_rows, n_cols)
    """
    n_rows, n_cols = df.count(), len(df.columns)
    shape = (n_rows, n_cols)
    return shape

def gender_dist(df):
    """
    Plots distribution of gender based on churn
    """
    trace_c = go.Bar(
        x = df.query(' isChurn ')['gender'],
        y = df.query(' isChurn ')['count'] / (df.query(' isChurn ')['count'].sum()),
        name = "Churn",
        marker = {
            'color': 'rgb(240, 133, 54)'
        }
    )

    trace_nc = go.Bar(
        x = df.query(' not isChurn ')['gender'],
        y = df.query(' not isChurn ')['count'] / (df.query(' not isChurn ')['count'].sum()),
        name = "Not Churn",
        marker = {
            'color': 'rgb(55, 119, 175)'
        }
    )

    data = [trace_c, trace_nc]

    layout = go.Layout(
        barmode= 'group'
    )

    fig = go.Figure(data = data, layout = layout)
    offline_py.iplot(fig)

def page_dist(df):
    """
    Plots distribution of pages
    """
    trace0 = go.Bar(
        x = df["page"].values,
        y = df["count"].values / df["count"].values.sum()
    )

    data = [trace0]
    layout = go.Layout(
        xaxis = {
            'automargin': True
        }
    )

    fig = go.Figure(data = data, layout = layout)

    offline_py.iplot(fig)

def page_dist_2(df):
    """
    Plots distribution of pages column with `NextSong` value filtered.
    """
    
    trace0 = go.Bar(
        x = df.query(' page != "NextSong" ')["page"].values,
        y = df.query(' page != "NextSong" ')["count"].values / df.query(' page != "NextSong" ')["count"].values.sum()
    )

    data = [trace0]

    layout = go.Layout(
        xaxis = {
            'automargin': True
        }
    )

    fig = go.Figure(data = data, layout = layout)

    offline_py.iplot(fig)
def page_dist_3(c_df, nc_df):
    """
    Plots distribution of pages visited based on churn
    """
    
    trace_c = go.Bar(
        x = c_df['page'],
        y = c_df['cFreq'],
        name = "Churn",
        marker = {
            'color': 'rgb(240, 133, 54)'
        }
    )

    trace_nc = go.Bar(
        x = nc_df['page'],
        y = nc_df['ncFreq'],
        name = "Not Churn",
        marker = {
            'color': 'rgb(55, 119, 175)'
        }
    )

    data = [trace_c, trace_nc]
    layout = go.Layout(
        barmode = 'group',
        xaxis = {
            'automargin': True,
        }
    )

    fig = go.Figure(data = data, layout = layout)

    offline_py.iplot(fig)

def plot_levels(df):
    """
    Shows proportions of how many paid users churned and how many free users
    churned.
    """
    trace = go.Bar(
        x = df["level"].value_counts().index,
        y = df["level"].value_counts().values / df.shape[0]
    )
    data = [trace]
    offline_py.iplot(data)

def plot_hod(c_df, nc_df):
    trace_c = go.Bar(
        x = c_df["hour"].values,
        y = c_df["count"].values / c_df["count"].sum(),
        name = "Churn",
        marker = {
            'color': 'rgb(240, 133, 54)'
        }
    )
    trace_nc = go.Bar(
        x = nc_df["hour"].values,
        y = nc_df["count"].values / nc_df["count"].sum(),
        name = "Not churn",
        marker = {
            'color': 'rgb(55, 119, 175)'
        }
    )

    data = [trace_c, trace_nc]

    layout = go.Layout(
        barmode = 'group',
        xaxis = {
            'title': 'Hour',
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 1.0,
            'tickcolor': '#000'
        },
        yaxis = {
            'title': 'Proportion',
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 0.01,
            'tickcolor': '#000'
        },
        shapes = [
            {
                'type': 'rect',
                'x0': 6.5,
                'y0': 0.001,
                'x1': 18.5,
                'y1': 0.04,
                'line': {
                    'color': 'rgb(255, 0, 0)',
                }
            }
        ]
    )

    fig = go.Figure(data=data, layout=layout)
    offline_py.iplot(fig)

def plot_diff_hod(c_df, nc_df):
    trace_diff = go.Bar(
        x = c_df["hour"].values,
        y = (nc_df["count"].values / nc_df["count"].sum()) - (c_df["count"].values / c_df["count"].sum()),
    )

    data = [trace_diff]
    layout = {
        'xaxis' : {
            'title': 'Hour',
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 1.0,
            'tickcolor': '#000'
        },
        'yaxis' : {
            'automargin': True,
            'title': 'Non-Churn Proportion minus<br> Churn Proportion <br>',
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 0.001,
            'tickcolor': '#000'
        },
    }

    fig = go.Figure(data=data, layout=layout)
    offline_py.iplot(fig)

def plot_dow(c_df, nc_df):
    trace_c = go.Bar(
        x = c_df["dow"].values,
        y = c_df["count"].values / c_df["count"].sum(),
        name = "Churn",
        marker = {
            'color': 'rgb(240, 133, 54)'
        }
    )
    trace_nc = go.Bar(
        x = nc_df["dow"].values,
        y = nc_df["count"].values / nc_df["count"].sum(),
        name = "Not churn",
        marker = {
            'color': 'rgb(55, 119, 175)'
        }
    )

    data = [trace_c, trace_nc]
    layout = {
        'barmode': 'group',
        'xaxis' : {
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 1.0,
            'tickcolor': '#000'
        },
        'yaxis' : {
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 0.01,
            'tickcolor': '#000'
        },
    }

    fig = go.Figure(data=data, layout=layout)
    offline_py.iplot(fig)

def plot_dom(c_df, nc_df):
    trace_c = go.Bar(
        x = c_df["dom"].values,
        y = c_df["count"].values / c_df["count"].sum(),
        name = "Churn",
        marker = {
            'color': 'rgb(240, 133, 54)'
        }
    )
    trace_nc = go.Bar(
        x = nc_df["dom"].values,
        y = nc_df["count"].values / nc_df["count"].sum(),
        name = "Not churn",
        marker = {
            'color': 'rgb(55, 119, 175)'
        }
    )

    data = [trace_c, trace_nc]
    layout = {
        'barmode': 'group',
        'xaxis' : {
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 1.0,
            'tickcolor': '#000'
        },
        'yaxis' : {
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 0.01,
            'tickcolor': '#000'
        },
    }

    fig = go.Figure(data=data, layout=layout)
    offline_py.iplot(fig)

def plot_diff_dom(c_df, nc_df):
    trace_diff = go.Bar(
        x = c_df["dom"].values,
        y = (nc_df["count"].values / nc_df["count"].sum()) - (c_df["count"].values / c_df["count"].sum()),
    )

    data = [trace_diff]
    layout = {
        'xaxis' : {
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 1.0,
            'tickcolor': '#000'
        },
        'yaxis' : {
            'tickmode': 'linear',
            'ticks': 'outside',
            'tick0': 0,
            'dtick': 0.01,
            'tickcolor': '#000'
        },
    }

    fig = go.Figure(data=data, layout=layout)
    offline_py.iplot(fig)

def plot_song_len(df):
    x = df["length"].dropna().values
    x = np.array(x)
    x = sorted(x)
    q1, q3 = np.quantile(x, 0.25), np.quantile(x, 0.75)

    trace1 = go.Histogram(
        x = x,
        xbins = dict(
            start = q1,
            end = q3,
            size = 0.1
        )
    )
    data = [trace1]

    fig = go.Figure(data = data)
    offline_py.iplot(fig)

def plot_devices(df):
    trace = go.Bar(
        x = df["device"].values,
        y = df["count"].values / (df["count"].values.sum())
    )

    data = [trace]

    fig = go.Figure(data = data)

    offline_py.iplot(fig)