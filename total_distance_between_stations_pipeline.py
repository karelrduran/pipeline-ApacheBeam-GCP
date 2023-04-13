""""Code challenge"""

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from geopy import distance


"""
  To run the pipeline:

  python -m codechallenge \
    --region europe-west1 \
    --input1 bigquery-public-data:london_bicycles.cycle_hire \
    --input2 bigquery-public-data:london_bicycles.cycle_stations \
    --output gs://<your-bucket-name>/output/outputs \
    --runner DataflowRunner \
    --project <your-project-name> \
    --temp_location gs://<your-bucket-name>/tmp/
"""


class GeodesicDistanceFn(beam.DoFn):
    """
      Calculate the distance between two points using the great circle distance fron geopy library
      besides calculate the total distance between two stations (distance_between_two_stations * amount_of_rides)  
    """

    def process(self, element):
        amount = float(element[2])
        distance_start_end = distance.great_circle((float(element[3]), float(
            element[4])), (float(element[5]), float(element[6]))).km
        total_distance = distance_start_end * amount
        return [{'start_station_id': element[0], 'end_station_id': element[1], 'amount_of_rides': element[2], 'total_distance_between_stations': total_distance}]


class InnerJoinFn(beam.DoFn):
    """
      Format the data: {'start_end_id': start_end_id, 'amount': amount, 'lats_lOns': station_lat)lon}
      taking just the rows with no empty data
    """

    def process(self, element):
        if (len(element[1]['amount_hires']) > 0) and (len(element[1]['station_lat_lon']) > 0):
            return [{'start_end_id': element[0], 'amount': element[1]['amount_hires'], 'lats_lons': element[1]['station_lat_lon']}]


class FormatFn(beam.DoFn):
    """
      Format the output: start_station_id,end_station_id,amount_of_rides,total_distance_between_stations
    """

    def process(self, element):
        for e in element:
            yield '{},{},{},{}'.format(e['start_station_id'], e['end_station_id'], e['amount_of_rides'], e['total_distance_between_stations'])


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the codechallenge pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input1',
        dest='input1',
        default='bigquery-public-data:london_bicycles.cycle_hire',
        help='Input london bicycle hires table to process.')
    parser.add_argument(
        '--input2',
        dest='input2',
        default='bigquery-public-data:london_bicycles.cycle_stations',
        help='Input london bicycle stations table to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='gs://<your-bucket-name>/output/outputs',
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        # Read cycle_hires table from BigQuery
        cycle_hires = p | 'ReadHireTable' >> beam.io.ReadFromBigQuery(
            table=known_args.input1)

        # Select start_station_id and end_station_id from cycle_hire, group and sum the total amount of rides.
        # Prepare the data with the structure [(start_station_id, end_station_id), amount_of_rides]
        amount_of_rides = (
            cycle_hires
            | 'SelectStartEnd' >> beam.Map(lambda row: ((row['start_station_id'], row['end_station_id']), 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
        )

        # Read cycle_stations table from BigQuery, make a Cross Join
        # And prepare the data with structure [(start_station_id, end_station_id),(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)]
        # for the Inner Join with amount_of_rides
        cycle_stations_cross_join = (
            p
            | 'ReadStationsTable' >> beam.io.ReadFromBigQuery(
                query="select t1.id start_station_id, t2.id end_station_id, t1.latitude start_station_latitude, t1.longitude start_station_longitude, t2.latitude end_station_latitude, t2.longitude end_station_longitude \
            from  `bigquery-public-data.london_bicycles.cycle_stations` t1 \
            cross join `bigquery-public-data.london_bicycles.cycle_stations` t2",
                use_standard_sql=True
            )
            | 'MapDictKeyCrossJoin' >> beam.Map(lambda row: ((row['start_station_id'], row['end_station_id']),
                                                             (row['start_station_latitude'], row['start_station_longitude'], row['end_station_latitude'], row['end_station_longitude'])))
        )

        # Group amount_of_rides and cycle_stations_cross_join PCollections by the common key
        # Apply the InnerJoin function to to avoid the empty rows
        # and prepare the data [(start_station_id, end_station_id, amount_of_rides, start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)]
        # to calculate the distance between stations
        group_amount_lat_lon = (
            {'amount_hires': amount_of_rides,
                'station_lat_lon': cycle_stations_cross_join}
            | 'GroupByCommonId' >> beam.CoGroupByKey()
            | 'InnerJoin' >> beam.ParDo(InnerJoinFn())
            | 'FormatInnerJoin' >> beam.Map(lambda result: (result['start_end_id'][0], result['start_end_id'][1], result['amount'][0],
                                                            result['lats_lons'][0][0], result['lats_lons'][0][1], result['lats_lons'][0][2], result['lats_lons'][0][3]))
        )

        # Calculate the distance betweem stations, sort and take 100 top distances
        geodesic_distance = (
            group_amount_lat_lon
            | 'GeodesicDistance' >> beam.ParDo(GeodesicDistanceFn())
            | 'SortDistanceTop100' >> beam.combiners.Top.Of(100, key=lambda elem: elem['total_distance_between_stations'], reverse=False)
        )

        # Format the output
        output = (
            geodesic_distance
            | 'FormatOutput' >> beam.ParDo(FormatFn())
        )

        # Write to output file
        output | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
