import pandas as pd
import numpy as np
import hdbscan

# Load the dataset
df = pd.read_parquet("training_set/20230327.parquet")
print(df.head())
# Aggressively sample 10,000 rows for faster testing
df_sample = df.sample(n=10000, random_state=42)

# Convert coordinates to a numpy array in radians (if using haversine-like logic)
coords = np.radians(df_sample[['lat', 'lon']].to_numpy())

# Initialize HDBSCAN with a minimum cluster size (HDBSCAN infers the density threshold automatically)
clusterer = hdbscan.HDBSCAN(min_cluster_size=5)
df_sample['cluster'] = clusterer.fit_predict(coords)

print(df_sample.head())
