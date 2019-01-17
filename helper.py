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