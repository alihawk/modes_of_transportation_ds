import networkx as nx  # type: ignore

def build_graph(df):
    """Build a transition graph between (spatial_bin, time_bin)."""
    G = nx.DiGraph()

    for device_id, group in df.groupby('deviceid', observed=False):
        group = group.sort_values('datetime')
        for (prev_row, next_row) in zip(group.iloc[:-1].itertuples(), group.iloc[1:].itertuples()):
            src = (prev_row.spatial_bin_id, prev_row.time_bin)
            dst = (next_row.spatial_bin_id, next_row.time_bin)
            if src != dst:
                if G.has_edge(src, dst):
                    G[src][dst]['weight'] += 1
                else:
                    G.add_edge(src, dst, weight=1)
    return G
