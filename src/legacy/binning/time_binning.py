from scripts.binning.params import TIME_BIN_MINUTES

def add_time_bin(df, minutes=TIME_BIN_MINUTES):
    """Assign time bin based on timestamp."""
    df['time_bin'] = (df['datetime'].dt.hour * 60 + df['datetime'].dt.minute) // minutes
    return df