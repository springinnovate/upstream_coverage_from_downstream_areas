"""Process upstream catchment areas from given downstream areas."""
import os
import logging
import sys

from ecoshard import geoprocessing
from ecoshard.geoprocessing import routing
from ecoshard import taskgraph
from osgeo import gdal

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOGGER.setLevel(logging.DEBUG)
logging.getLogger('ecoshard.fetch_data').setLevel(logging.INFO)


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

DEM_RASTER_PATH = r"D:\repositories\upstream_coverage_from_downstream_areas\data\global_dem.tif"

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
    task_graph = taskgraph.TaskGraph(INTERMEDIATE_DIR, len(RASTERS_TO_PROCESS))
    dem_info = geoprocessing.get_raster_info(DEM_RASTER_PATH)
    for raster_path in RASTERS_TO_PROCESS:
        raster_info = geoprocessing.get_raster_info(raster_path)

        local_bounding_box = geoprocessing.merge_bounding_box_list(
            [raster_info['bounding_box'], dem_info['bounding_box']],
            'intersection')

        LOGGER.info(f'processing {raster_path}')
        local_working_dir = os.path.join(
            INTERMEDIATE_DIR,
            os.path.basename(os.path.splitext(raster_path)[0]))
        aligned_dem_path = os.path.join(
            local_working_dir, os.path.basename(DEM_RASTER_PATH))
        aligned_raster_path = os.path.join(
            local_working_dir, os.path.basename(raster_path))

        align_dem_task = task_graph.add_task(
            func=geoprocessing.warp_raster,
            args=(
                DEM_RASTER_PATH, dem_info['pixel_size'], aligned_dem_path,
                'near'),
            kwargs={'target_bb': local_bounding_box},
            target_path_list=[aligned_dem_path],
            task_name=f'align {DEM_RASTER_PATH}')
        align_raster_task = task_graph.add_task(
            func=geoprocessing.warp_raster,
            args=(
                raster_path, dem_info['pixel_size'], aligned_raster_path,
                'near'),
            kwargs={'target_bb': local_bounding_box},
            target_path_list=[aligned_raster_path],
            task_name=f'align {raster_path}')
        flow_dir_path = os.path.join(local_working_dir, 'flow_dir.tif')
        raster_info = geoprocessing.get_raster_info(raster_path)
        flow_dir_task = task_graph.add_task(
            func=routing.flow_dir_mfd,
            args=((aligned_dem_path, 1), flow_dir_path),
            kwargs={'working_dir': local_working_dir},
            dependent_task_list=[align_dem_task],
            target_path_list=[flow_dir_path],
            task_name=f'flow dir for {flow_dir_path}')
        channel_raster_proxy_path = os.path.join(
            local_working_dir, 'channel_proxy.tif')
        channel_proxy_task = task_graph.add_task(
            func=geoprocessing.raster_calculator,
            args=(
                [(aligned_raster_path, 1),
                 (raster_info['nodata'][0], 'raw'),
                 (MASK_NODATA, 'raw')],
                _convert_to_mask_op,
                channel_raster_proxy_path,
                gdal.GDT_Byte,
                MASK_NODATA),
            dependent_task_list=[align_raster_task],
            target_path_list=[channel_raster_proxy_path],
            task_name=f'channel proxy {channel_raster_proxy_path}')
        distance_to_channel_path = os.path.join(
            local_working_dir, 'dist_to_channel.tif')
        dist_to_channel_task = task_graph.add_task(
            func=routing.distance_to_channel_mfd,
            args=(
                (flow_dir_path, 1), (channel_raster_proxy_path, 1),
                distance_to_channel_path),
            dependent_task_list=[flow_dir_task, channel_proxy_task],
            target_path_list=[distance_to_channel_path],
            task_name=f'dist to channel {distance_to_channel_path}')
        upstream_coverage_raster_path = os.path.join(
            WORKSPACE_DIR,
            f'upstream_coverage_{os.path.basename(raster_path)}')
        upstream_coverage_task = task_graph.add_task(
            func=geoprocessing.raster_calculator,
            args=(
                [(channel_raster_proxy_path, 1),
                 (distance_to_channel_path, 1),
                 (MASK_NODATA, 'raw'),
                 (-1, 'raw'),  # I know dist to channel nodata is -1
                 (MASK_NODATA, 'raw')],
                _join_masks_op,
                upstream_coverage_raster_path,
                gdal.GDT_Byte,
                MASK_NODATA),
            dependent_task_list=[dist_to_channel_task, channel_proxy_task],
            target_path_list=[upstream_coverage_raster_path],
            task_name=f'upstream coverage {upstream_coverage_raster_path}')

    task_graph.join()
    task_graph.close()
    LOGGER.info('all done!')


if __name__ == '__main__':
    main()
