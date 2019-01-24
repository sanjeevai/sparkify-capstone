# imports
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

def page_dist(df):
    """
    Plots distribution of pages without filtering anything from dataframe

    Args:
    df pandas.core.frame.DataFrame: Pandas dataframe with two columns - "page" 
        and "count"

    Returns:
    None
    """

    trace0 = go.Bar(
        x = df["page"].values,
        y = 100 * df["count"].values / df["count"].values.sum()
    )

    data = [trace0]
    layout = go.Layout(
        title = "DISTRIBUTION OF <br> PAGES",
        xaxis = {
            "title": "TYPE OF <br> PAGE",
            "automargin": True
        },
        yaxis = {
            "title": "PERCENTAGE",
            "automargin": True,
        }
    )

    fig = go.Figure(data = data, layout = layout)

    offline_py.iplot(fig)

def page_dist_2(df):
    """
    Plots distribution of pages column with `NextSong` value filtered from data.

    Args:
    df pandas.core.frame.DataFrame: Same argument as function page_dist_2()

    Returns:
    None
    """
    
    trace0 = go.Bar(
        x = df.query(' page != "NextSong" ')["page"].values,
        y = 100 * df.query(' page != "NextSong" ')["count"].values / \
            df.query(' page != "NextSong" ')["count"].values.sum()
    )

    data = [trace0]

    layout = go.Layout(
        title = "DISTRIBUTION OF <br> PAGES",
        xaxis = {
            "title": "TYPE OF <br> PAGE",
            "automargin": True
        },
        yaxis = {
            "title": "PERCENTAGE",
            "automargin": True,
        },
    )

    fig = go.Figure(data = data, layout = layout)

    offline_py.iplot(fig)

def gender_dist(df):
    """
    Plots distribution of gender based on churn

    Args:
    df pandas.core.frame.DataFrame: Dataframe with three columns- isChurn(bool),
        gender(str) and count(int)

    Returns:
    None
    """

    trace_c = go.Bar(
        x = df.query(' isChurn ')["gender"],
        y = 100 * df.query(' isChurn ')["count"] / (df.query(' isChurn ') \
            ["count"].sum()),
        name = "Churn",
        marker = {
            "color": "rgb(240, 133, 54)"
        }
    )

    trace_nc = go.Bar(
        x = df.query(' not isChurn ')["gender"],
        y = 100 * df.query(' not isChurn ')["count"] / \
            (df.query(' not isChurn ')["count"].sum()),
        name = "Not Churn",
        marker = {
            "color": "rgb(55, 119, 175)",
        }
    )

    data = [trace_c, trace_nc]

    layout = go.Layout(
        title = "DISTRIBUTION OF GENDER <br> BASED ON CHURN",
        xaxis = {
            "title": "GENDER"
        },
        yaxis = {
            "title": "PERCENTAGE"
        },
        barmode = "group"
    )

    fig = go.Figure(data = data, layout = layout)
    offline_py.iplot(fig)

def page_dist_3(c_df, nc_df):
    """
    Plots distribution of pages visited based on churn

    Args:
    c_df pandas.core.frame.DataFrame: Pandas dataframe of churn user. Contains
        three columns - page type, count and proportion
    nc_df pandas.core.frame.DataFrame: Pandas dataframe of non-churn user. 
        Contains three columns - page type, count and proportion

    Returns:
    None
    """
    
    trace_c = go.Bar(
        x = c_df["page"],
        y = c_df["pCount"],
        name = "Churn",
        marker = {
            "color": "rgb(240, 133, 54)"
        }
    )

    trace_nc = go.Bar(
        x = nc_df["page"],
        y = nc_df["pCount"],
        name = "Not Churn",
        marker = {
            "color": "rgb(55, 119, 175)"
        }
    )

    data = [trace_c, trace_nc]
    
    layout = go.Layout(
        title = "DISTRIBUTION OF <br> PAGES",
        barmode = "group",
        xaxis = {
            "title": "TYPE OF <br> PAGE",
            "automargin": True,
        },
        yaxis = {
            "title": "PERCENTAGE",
            "automargin": True,
        },
    )

    fig = go.Figure(data = data, layout = layout)

    offline_py.iplot(fig)



def plot_levels(df):
    """
    Shows proportions of how many paid users churned and how many free users
    churned.

    Args:
    df pandas.core.frame.DataFrame: Pandas dataframe of one column - 
        subscription status with two levels: paid and free

    Returns:
    None
    """

    trace = go.Bar(
        x = df["level"].value_counts().index,
        y = 100 * df["level"].value_counts().values / df.shape[0]
    )
    
    layout = go.Layout(
        title = "DISTRIBUTION OF LEVEL",
        xaxis = {
            "title": "LEVEL",
        },
        yaxis = {
            "title": "PERCENTAGE",
        },
    )

    data = [trace]
    
    fig = go.Figure(data = data, layout = layout)

    offline_py.iplot(fig)

def plot_hod(c_df, nc_df):
    """
    Plots hour distribution based on churn status of user

    Args:
    c_df pandas.core.frame.DataFrame: Data frame of churn users with two columns
        - "hour" with hour value and "count" with how many time events occurred at
        that hour 
    nc_df pandas.core.frame.DataFrame: Data frame of non-churn users with two
        columns - "hour" with hour value and "count" with how many time events
        occurred at that hour
    
    Returns:
    None
    """

    trace_c = go.Bar(
        x = c_df["hour"].values,
        y = c_df["count"].values / c_df["count"].sum(),
        name = "Churn",
        marker = {
            "color": "rgb(240, 133, 54)"
        }
    )
    trace_nc = go.Bar(
        x = nc_df["hour"].values,
        y = nc_df["count"].values / nc_df["count"].sum(),
        name = "Not churn",
        marker = {
            "color": "rgb(55, 119, 175)"
        }
    )

    data = [trace_c, trace_nc]

    layout = go.Layout(
        title = "DISTRIBUTION OF HOUR <br> BASED ON CHURN",
        barmode = "group",
        xaxis = {
            "title": "HOUR",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 1.0,
            "tickcolor": "#000"
        },
        yaxis = {
            "title": "PROPORTION",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 0.01,
            "tickcolor": "#000"
        },
        shapes = [
            {
                "type": "rect",
                "x0": 6.5,
                "y0": 0.001,
                "x1": 18.5,
                "y1": 0.04,
                "line": {
                    "color": "rgb(255, 0, 0)",
                }
            }
        ],
    )

    fig = go.Figure(data=data, layout=layout)
    
    offline_py.iplot(fig)

def plot_diff_hod(c_df, nc_df):
    """
    Plots difference of hour distribution based on churn status of user

    Args:
    c_df pandas.core.frame.DataFrame: Data frame of churn users with two columns
        - "hour" with hour value and "count" with how many time events occurred at
        that hour 
    nc_df pandas.core.frame.DataFrame: Data frame of non-churn users with two
        columns - "hour" with hour value and "count" with how many time events
        occurred at that hour
    
    Returns:
    None
    """

    trace_diff = go.Bar(
        x = c_df["hour"].values,
        y = (nc_df["count"].values / nc_df["count"].sum())  - \
            (c_df["count"].values / c_df["count"].sum()),
    )

    data = [trace_diff]

    layout = go.Layout(
        title = "DIFFERENCE BETWEEN NON-CHURN AND CHURN USERS <br> \
                 HOUR DISTRIBUTION",
        xaxis = {
            "title": "HOUR",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 1.0,
            "tickcolor": "#000"
        },
        yaxis = {
            "automargin": True,
            "title": "NON-CHURN PROPORTION MINUS <br> CHURN PROPORTION",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 0.001,
            "tickcolor": "#000"
        },
    )

    fig = go.Figure(data=data, layout=layout)
    
    offline_py.iplot(fig)

def plot_dow(c_df, nc_df):
    """
    Plots distribution at week level based on churn status of the user

    Args:
    c_df pandas.core.frame.DataFrame: Data frame of churn users with two columns
        - "dow" with weekday value(0 for Monday) and "count" with how many events 
        occurred on that day
    nc_df pandas.core.frame.DataFrame: Data frame of non-churn users with two
        columns - "dom" with day value and "count" with how many events occurred 
        on that day
    
    Returns:
    None
    """

    trace_c = go.Bar(
        x = c_df["dow"].values,
        y = 100 * c_df["count"].values / c_df["count"].sum(),
        name = "Churn",
        marker = {
            "color": "rgb(240, 133, 54)"
        }
    )
    trace_nc = go.Bar(
        x = nc_df["dow"].values,
        y = 100 * nc_df["count"].values / nc_df["count"].sum(),
        name = "Not churn",
        marker = {
            "color": "rgb(55, 119, 175)"
        }
    )

    data = [trace_c, trace_nc]
    layout = go.Layout(
        barmode = "group",
        title = "DISTRIBUTION OF WEEKDAY <br> BASED ON CHURN",
        xaxis = {
            "title": "WEEKDAY (0 IS MONDAY)",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 1.0,
            "tickcolor": "#000"
        },
        yaxis = {
            "title": "PERCENTAGE",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 2,
            "tickcolor": "#000"
        },
    )

    fig = go.Figure(data=data, layout=layout)
    
    offline_py.iplot(fig)

def plot_dom(c_df, nc_df):
    """
    Plots distribution at month level based on churn status of the user

    Args:
    c_df pandas.core.frame.DataFrame: Data frame of churn users with two columns
        - "dom" with day number and "count" with how many events occurred on that 
        day
    nc_df pandas.core.frame.DataFrame: Data frame of non-churn users with two
        columns - "dom" with day number value and "count" with how many events 
        occurred on that day
    
    Returns:
    None
    """
    trace_c = go.Bar(
        x = c_df["dom"].values,
        y = c_df["count"].values / c_df["count"].sum(),
        name = "Churn",
        marker = {
            "color": "rgb(240, 133, 54)"
        }
    )
    trace_nc = go.Bar(
        x = nc_df["dom"].values,
        y = nc_df["count"].values / nc_df["count"].sum(),
        name = "Not churn",
        marker = {
            "color": "rgb(55, 119, 175)"
        }
    )

    data = [trace_c, trace_nc]
    
    layout = go.Layout(
        barmode = "group",
        title = "DISTRIBUTION OF DAY OF MONTH <br> BASED ON CHURN",
        xaxis = {
            "title": "DAY OF MONTH",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 1.0,
            "tickcolor": "#000"
        },
        yaxis = {
            "title": "PROPORTION",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 0.01,
            "tickcolor": "#000"
        },
    )

    fig = go.Figure(data=data, layout=layout)
    
    offline_py.iplot(fig)

def plot_diff_dom(c_df, nc_df):
    """
    Plots difference in distributions at month level based on churn status of 
    the user

    Args:
    c_df pandas.core.frame.DataFrame: Data frame of churn users with two columns
        - "dom" with day number and "count" with how many events occurred on that 
        day
    nc_df pandas.core.frame.DataFrame: Data frame of non-churn users with two
        columns - "dom" with day number value and "count" with how many events 
        occurred on that day
    
    Returns:
    None
    """
    
    trace = go.Bar(
        x = c_df["dom"].values,
        y = (nc_df["count"].values / nc_df["count"].sum()) - \
            (c_df["count"].values / c_df["count"].sum()),
    )

    data = [trace]
    
    layout = go.Layout(
        title = "DIFFERENCE BETWEEN NON-CHURN AND CHURN USERS <br> DAY DISTRIBUTION",
        xaxis = {
            "title": "DAY OF MONTH",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 1.0,
            "tickcolor": "#000"
        },
        yaxis = {
            "title": "NON-CHURN PROPORTION MINUS <br> CHURN PROPORTION",
            "tickmode": "linear",
            "ticks": "outside",
            "tick0": 0,
            "dtick": 0.01,
            "tickcolor": "#000"
        },
    )

    fig = go.Figure(data=data, layout=layout)

    offline_py.iplot(fig)

def plot_song_len(df):
    """
    Plot distribution of song length

    Args:
    df pandas.core.frame.DataFrame: Dataframe with all features, it will extract
    the length column, IQR and then plot a histogram with appropriate bin-width

    Returns:
    None
    """
    x = df["length"].dropna().values
    x = np.array(x)
    x = sorted(x)
    
    q1, q3 = np.quantile(x, 0.25), np.quantile(x, 0.75)

    trace = go.Histogram(
        x = x,
        xbins = dict(
            start = q1,
            end = q3,
            size = 0.1
        )
    )
    
    layout = go.Layout(
        title = "SONG LENGTH DISTRIBUTION",
        xaxis = {
            "title": "LENGTH OF SONG <br> (IN SECONDS)",
        },
        yaxis = {
            "title": "COUNT",
        },
    )
    
    data = [trace]

    fig = go.Figure(data = data, layout = layout)
    
    offline_py.iplot(fig)

def plot_devices(df):
    """
    Plots distribution of device used for streaming

    Args:
    df pandas.core.frame.DataFrame: Dataframe with two columns- "device" with
        device name and 'count" with how many that device was used.

    Returns:
    None
    """
    trace = go.Bar(
        x = df["device"].values,
        y = 100 * df["count"].values / (df["count"].values.sum())
    )

    data = [trace]
    
    layout = go.Layout(
        title = "TYPES OF DEVICE USED",
        xaxis = {
            "title": "DEVICE TYPE",
        },
        yaxis = {
            "title": "PERCENTAGE",
        },
    )

    fig = go.Figure(data = data, layout = layout)

    offline_py.iplot(fig)

def churn_dist(df):
    """
    Plots the distribution of churn to explaing the imbalance in data set.

    Args:
    df pandas.core.frame.DataFrame: 
    Returns:
    None
    """
    trace = go.Bar(
        x = [bool(df["isChurn"].values[0]), bool(df["isChurn"].values[1])],
        y = 100 * df["count"].values / (df["count"].values.sum())
    )

    data = [trace]

    layout = go.Layout(
        title = "Distribution of Churn",
        xaxis = {
            "title": "CHURN STATUS",
            "tick0": 0,
            "dtick": 1,
        },
        yaxis = {
            "title": "PERCENTAGE",
            "tick0": 0,
            "dtick": 20,
        }
    )

    fig = go.Figure(data = data, layout = layout)

    offline_py.iplot(fig)