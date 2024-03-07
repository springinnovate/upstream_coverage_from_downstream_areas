"""Process upstream catchment areas from given downstream areas."""
import os

from ecoshard import geoprocessing
from ecoshard.geoprocessing import routing
from ecoshard import taskgraph
from osgeo import gdal

WORKSPACE_DIR = 'workspace'
INTERMEDIATE_DIR = os.path.join(WORKSPACE_DIR, 'intermediate_dir')
os.makedirs(INTERMEDIATE_DIR, exist_ok=True)

RASTERS_TO_PROCESS = [
    r"D:\repositories\upstream_coverage_from_downstream_areas\data\River_Flooding_RCP45_Top_Decile_Vulnerability.tif",
    r"D:\repositories\upstream_coverage_from_downstream_areas\data\River_Flooding_RCP45_Total_Vulnerability-004.tif",
    r"D:\repositories\upstream_coverage_from_downstream_areas\data\River_Flooding_RCP85_Top_Decile_Vulnerability.tif",
    r"D:\repositories\upstream_coverage_from_downstream_areas\data\River_Flooding_RCP85_Total_Vulnerability-003.tif",
    r"D:\repositories\upstream_coverage_from_downstream_areas\data\Water_Stress_RCP45_Top_Decile_Vulnerability.tif",
    r"D:\repositories\upstream_coverage_from_downstream_areas\data\Water_Stress_RCP45_Total_Vulnerability-003.tif",
    r"D:\repositories\upstream_coverage_from_downstream_areas\data\Water_Stress_RCP85_Top_Decile_Vulnerability-002.tif",
    r"D:\repositories\upstream_coverage_from_downstream_areas\data\Water_Stress_RCP85_Total_Vulnerability-001.tif",
]

DEM_RASTER_PATH = r"D:\repositories\wwf-sipa\data\aster_dem\aster_dem.vrt"

MASK_NODATA = 2


def _convert_to_mask_op(array, base_nodata, target_nodata):
    result = array > 0
    result[array == base_nodata] = target_nodata
    return result


def _join_masks_op(mask_a, mask_b, a_nodata, b_nodata, target_nodata):
    result = (mask_a > 0) | (mask_b > 0)
    nodata_mask = (mask_a == a_nodata) & (mask_b == b_nodata)
    result[nodata_mask] = target_nodata
    return result


def main():
    """Entry point."""
    dem_info = geoprocessing.get_raster_info(DEM_RASTER_PATH)
    for raster_path in RASTERS_TO_PROCESS:
        local_working_dir = os.path.join(
            INTERMEDIATE_DIR,
            os.path.basename(os.path.splitext(raster_path)[0]))
        aligned_dem_path = os.path.join(
            local_working_dir, os.path.basename(DEM_RASTER_PATH))
        aligned_raster_path = os.path.join(
            local_working_dir, os.path.basename(raster_path))
        geoprocessing.align_and_resize_raster_stack(
            [raster_path, DEM_RASTER_PATH],
            [aligned_dem_path, aligned_raster_path], ['near', 'near'],
            dem_info['pixel_size'], 'intersection')
        flow_dir_path = os.path.join(local_working_dir, 'flow_dir.tif')
        raster_info = geoprocessing.get_raster_info(raster_path)
        routing.flow_dir_d8(
            (aligned_dem_path, 1), flow_dir_path,
            working_dir=local_working_dir)
        channel_raster_proxy_path = os.path.join(
            local_working_dir, 'channel_proxy.tif')
        geoprocessing.raster_calculator(
            [(channel_raster_proxy_path, 1),
             (raster_info['nodata'][0], 'raw'),
             (raster_info['nodata'][0], 'raw'),
             (MASK_NODATA, 'raw')],
            _convert_to_mask_op, channel_raster_proxy_path, gdal.GDT_Byte,
            [MASK_NODATA])
        distance_to_channel_path = os.path.join(
            local_working_dir, 'dist_to_channel.tif')
        routing.distance_to_channel_d8(
            (flow_dir_path, 1), (channel_raster_proxy_path, 1),
            distance_to_channel_path)
        upstream_coverage_raster_path = os.path.join(
            WORKSPACE_DIR,
            f'upstream_coverage_{os.path.basename(raster_path)}')
        geoprocessing.raster_calculator(
            [(channel_raster_proxy_path, 1),
             (distance_to_channel_path, 1),
             (MASK_NODATA, 'raw'),
             (-1, 'raw'),  # I know dist to channel nodata is -1
             (MASK_NODATA, 'raw')],
            _join_masks_op, upstream_coverage_raster_path,
            gdal.GDT_Byte, [MASK_NODATA])


if __name__ == '__main__':
    main()
