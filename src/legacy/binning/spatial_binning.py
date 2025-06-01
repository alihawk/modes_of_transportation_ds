from scripts.common_imports import *

def add_zone_id(df, grid_path="maps/binning_2000_m2.geojson"):
    """Assign each point (lon, lat) to a zone_id from the spatial grid."""
    
    # Load your spatial grid
    grid_gdf = gpd.read_file(grid_path)
    
    # Ensure grid has unique IDs
    if "zone_id" not in grid_gdf.columns:
        grid_gdf["zone_id"] = np.arange(len(grid_gdf))
    
    # Create points from your df
    points_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")
    
    # Ensure both are in the same CRS
    if grid_gdf.crs != points_gdf.crs:
        grid_gdf = grid_gdf.to_crs(points_gdf.crs)
    
    # Spatial join: assign zone_id
    joined = gpd.sjoin(points_gdf, grid_gdf[["zone_id", "geometry"]], how="left", predicate="within")
    
    # Drop the geometry after assignment
    joined = joined.drop(columns="geometry")
    
    return joined
