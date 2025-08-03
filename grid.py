import geopandas as gpd
import numpy as np
from shapely.geometry import Point
import math
import folium
from folium.plugins import MarkerCluster
from scipy.spatial import cKDTree # New import for distance calculation

def main():
    """
    Main function to orchestrate the coordinate generation process.
    """
    # --- 1. Configuration & Constants ---
    print("Initializing configuration...")
    # --- Core Parameters ---
    NYC_LAT, NYC_LON = 40.7128, -74.0060
    RADIUS_MILES = 105
    TARGET_POINTS = 9970
    MIN_SPACING_METERS = 500  # Minimum allowed spacing between grid points

    # --- System Constants ---
    MILES_TO_METERS = 1609.34
    # Use a projected Coordinate Reference System (CRS) for accurate distance calculations in meters.
    # EPSG:32618 is UTM Zone 18N, appropriate for the NYC area.
    PROJECTED_CRS = "EPSG:32618"
    GEOGRAPHIC_CRS = "EPSG:4326" # WGS84 for standard lat/lon
    SHAPEFILE_PATH = "ne_10m_land.shp"

    # --- 2. Define Area of Interest (AOI) ---
    print(f"\nStep 1: Creating a {RADIUS_MILES}-mile buffer around NYC...")
    center_point_gdf = gpd.GeoDataFrame(geometry=[Point(NYC_LON, NYC_LAT)], crs=GEOGRAPHIC_CRS)
    center_point_projected = center_point_gdf.to_crs(PROJECTED_CRS)
    radius_meters = RADIUS_MILES * MILES_TO_METERS
    aoi_buffer_gdf = gpd.GeoDataFrame(geometry=center_point_projected.buffer(radius_meters), crs=PROJECTED_CRS)
    print("AOI created successfully.")

    # --- 3. Calculate Land Area and Determine Grid Spacing ---
    print("\nStep 2: Calculating land area and required grid spacing...")
    try:
        land_gdf = gpd.read_file(SHAPEFILE_PATH)
    except Exception:
        print(f"\nFATAL ERROR: Could not find '{SHAPEFILE_PATH}'.")
        print("Please download and unzip the '10m Land' shapefile from Natural Earth into the same folder.")
        print("Link: https://www.naturalearthdata.com/downloads/10m-physical-vectors/10m-land/")
        return

    # Pre-clip the global land data to the AOI's bounding box to improve performance and stability.
    print("Pre-clipping global land data to the AOI...")
    bbox_gdf = gpd.GeoDataFrame(geometry=[aoi_buffer_gdf.union_all().envelope], crs=PROJECTED_CRS)
    clipped_land = land_gdf.clip(bbox_gdf.to_crs(land_gdf.crs))
    land_projected = clipped_land.to_crs(PROJECTED_CRS)

    # Calculate the intersection to find the exact landmass within the buffer.
    print("Calculating precise land area within the buffer...")
    land_in_buffer = gpd.overlay(land_projected, aoi_buffer_gdf, how='intersection')
    total_land_area = land_in_buffer.area.sum()
    print(f"Total land area in buffer: {total_land_area / 1e6:.2f} sq km")

    # Determine the grid spacing, enforcing the minimum.
    if total_land_area > 0:
        calculated_spacing = math.sqrt(total_land_area / TARGET_POINTS)
        print(f"Ideal spacing for ~{TARGET_POINTS} points: {calculated_spacing:.2f} meters")
        
        grid_spacing_meters = max(calculated_spacing, MIN_SPACING_METERS)

        if grid_spacing_meters == MIN_SPACING_METERS and calculated_spacing < MIN_SPACING_METERS:
            print(f"WARNING: Ideal spacing ({calculated_spacing:.2f}m) is below the minimum of {MIN_SPACING_METERS}m.")
            print(f"Using {MIN_SPACING_METERS}m spacing. The final point count will likely be lower than the target.")
        else:
            print(f"Using final grid spacing of: {grid_spacing_meters:.2f} meters")
    else:
        print("No land found in the specified buffer. Exiting.")
        return

    # --- 4. Generate and Filter Grid Points ---
    print("\nStep 3: Generating and filtering grid points...")
    min_x, min_y, max_x, max_y = aoi_buffer_gdf.total_bounds
    x_coords = np.arange(min_x, max_x, grid_spacing_meters)
    y_coords = np.arange(min_y, max_y, grid_spacing_meters)
    xx, yy = np.meshgrid(x_coords, y_coords)

    grid_points = gpd.GeoDataFrame(
        geometry=[Point(x, y) for x, y in zip(xx.flatten(), yy.flatten())],
        crs=PROJECTED_CRS,
    )
    print(f"Generated a raw grid with {len(grid_points)} points.")

    # Filter points to keep only those on the land within the circular buffer.
    print("Performing spatial join to isolate points on land... (This may take a minute)")
    clipped_grid_to_buffer = grid_points[grid_points.within(aoi_buffer_gdf.union_all())]
    points_on_land = gpd.sjoin(clipped_grid_to_buffer, land_in_buffer, how="inner", predicate="intersects")
    
    # Remove duplicates that can arise from the spatial join.
    points_on_land = points_on_land[~points_on_land.index.duplicated(keep='first')]
    points_on_land = points_on_land[['geometry']]
    print(f"Filtered down to {len(points_on_land)} final points on land.")

    if points_on_land.empty:
        print("No points were generated on land. Exiting.")
        return

    # --- 5. Calculate Average Distance Between Points ---
    print("\nStep 4: Calculating average distance between final points...")
    avg_distance = 0
    if len(points_on_land) > 1:
        # Extract coordinates into a NumPy array for high-performance calculation.
        coords = np.array([point.coords[0] for point in points_on_land.geometry])
        
        # Build a k-d tree for efficient nearest-neighbor search.
        tree = cKDTree(coords)
        
        # Query for the 2 nearest neighbors. The first (k=1) is the point itself.
        # The second (k=2) is the actual nearest neighbor.
        distances, _ = tree.query(coords, k=2, workers=-1) # Use all available cores
        
        # The nearest neighbor distances are in the second column (index 1).
        nearest_distances = distances[:, 1]
        avg_distance = np.mean(nearest_distances)
    else:
        print("Not enough points to calculate average distance.")

    # --- 6. Finalize Data and Save to CSV ---
    print("\nStep 5: Saving coordinates to CSV...")
    final_points_wgs84 = points_on_land.to_crs(GEOGRAPHIC_CRS)
    final_points_wgs84['latitude'] = final_points_wgs84.geometry.y
    final_points_wgs84['longitude'] = final_points_wgs84.geometry.x
    output_df = final_points_wgs84[['longitude', 'latitude']]
    
    output_filename = 'nyc_land_points_final.csv'
    output_df.to_csv(output_filename, index=False)
    print(f"Coordinates saved to '{output_filename}'")

    # --- 7. Generate and Save Interactive HTML Map ---
    print("\nStep 6: Generating interactive HTML map...")
    map_filename = 'nyc_points_map_final.html'
    m = folium.Map(location=[NYC_LAT, NYC_LON], zoom_start=8, tiles="CartoDB positron")
    marker_cluster = MarkerCluster(name="NYC Area Points").add_to(m)

    for _, row in output_df.iterrows():
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=2,
            color="#0078A8",
            fill=True,
            fill_opacity=0.7,
            popup=f"Lat: {row['latitude']:.4f}<br>Lon: {row['longitude']:.4f}"
        ).add_to(marker_cluster)
    
    folium.LayerControl().add_to(m)
    m.save(map_filename)
    print(f"Map saved to '{map_filename}'")

    # --- Final Summary ---
    print("\n--- 🚀 Process Complete! 🚀 ---")
    print(f"Target Points:                {TARGET_POINTS}")
    print(f"Final Points Generated:       {len(output_df)}")
    print(f"Final Grid Spacing Used:      {grid_spacing_meters:.2f} meters")
    print(f"Avg. Distance (Nearest-N):    {avg_distance:.2f} meters")
    print("---------------------------------")


if __name__ == "__main__":
    main()