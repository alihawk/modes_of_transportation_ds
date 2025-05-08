from scripts.common_imports import *
from scripts.binning.params import SPATIAL_GRID_SIZE

# Discretize the country using the original zoning.geojson

# Load minimalist_coning.geojson
input_path = Path("maps/minimalist_coning.geojson")
output_path_geo = Path(f"maps/binning_{SPATIAL_GRID_SIZE}_m2.geojson")
output_path_html = Path(f"maps/binning_{SPATIAL_GRID_SIZE}_m2.html")

logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

if output_path_geo.exists():
    logging.info(f"GeoJSON for {SPATIAL_GRID_SIZE}_m2 already created and stored at {output_path_geo}")
    sys.exit(0)

slovenia = gpd.read_file(input_path) # type: ignore
slovenia = slovenia.to_crs(epsg=3857)

# Get bounds of Slovenia based on 
minx, miny, maxx, maxy = slovenia.total_bounds

# Set bin size in meters
bin_size = SPATIAL_GRID_SIZE

# Create grid cells
x_coords = np.arange(minx, maxx, bin_size) # type: ignore
y_coords = np.arange(miny, maxy, bin_size) # type: ignore
logging.info("Creating polygons list")
polygons = [box(x, y, x + bin_size, y + bin_size) for x in x_coords for y in y_coords] # type: ignore
logging.info("Adding to GeoDataFrame")
grid = gpd.GeoDataFrame(geometry=polygons, crs=slovenia.crs) # type: ignore

# Clip to Slovenia boundary and go back to lat and lon measures
logging.info("Clipping to Slovenia boundary")
grid_selected = gpd.sjoin(grid, slovenia, how='inner', predicate='intersects') # type: ignore
logging.info("Going back to lat and lon measurements")
grid_selected = grid_selected.to_crs(epsg=4326)
grid_selected = grid_selected.drop(columns=['index_right'])
grid_selected["ID"] = range(len(grid_selected))

#Saving the file
logging.info("Saving GeoJSON")
grid_selected.to_file(output_path_geo, driver="GeoJSON")

#Saving visualization of the grid
m= folium.Map(location=[46, 14]) # type: ignore
folium.GeoJson( # type: ignore
    grid_selected,
).add_to(m)
logging.info("Saving HTML")
m.save(output_path_html)