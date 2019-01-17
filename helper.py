import plotly.graph_objs as go
import plotly.offline as offline_py

def spark_shape(df):
    """
    Return a tuple representing the dimensionality of the Spark Dataframe
    
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
    Plots distribution of churn based on gender
    """
    trace_c = go.Bar(
        x = df.query(' isChurn ')['gender'],
        y = df.query(' isChurn ')['count'],
        name = "Churn"
    )

    trace_nc = go.Bar(
        x = df.query(' not isChurn ')['gender'],
        y = df.query(' not isChurn ')['count'],
        name = "Not Churn"
    )

    data = [trace_c, trace_nc]

    layout = go.Layout(
        barmode= 'group'
    )

    fig = go.Figure(data = data, layout = layout)
    offline_py.iplot(fig)