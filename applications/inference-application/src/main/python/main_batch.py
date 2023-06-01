from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream import TimeCharacteristic
from pyflink.table import DataTypes, Slide
from pyflink.table.udf import udf
from feature_extractors.functions import doubles, strings, integers, labels

class RefitFeatureEnrichment():
    def __init__(self):
        self.settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(1)
        self.table_env = StreamTableEnvironment.create(self.env, environment_settings=self.settings)
        self.table_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)
        self.table_env.add_python_file('feature_extractors')

        source_table = open('feature_extractors/source.sql', 'r').read()
        sink_table = open('feature_extractors/sink.sql', 'r').read()

        self.table_env.execute_sql(source_table)
        self.table_env.execute_sql(sink_table)

    def run_udf(self):
        # Register the UDFs
        self.table_env.register_function("doubles", doubles)
        self.table_env.register_function("strings", strings)
        self.table_env.register_function("integers", integers)
        self.table_env.register_function("labels", labels)

        # Set the time characteristic to EventTime
        self.env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

        # Use a tumbling event-time window
        window_size = 60  # 1 minute window
        window_slide = Slide.over(window_size).every(window_size).on("timestamp").alias("w")  # Define window slide

        # Apply the UDFs on the windowed stream
        windowed_table = self.table_env.from_path('refit_raw_sensor_data') \
            .group_by("projectGuid, sensorId, w") \
            .select("projectGuid, "
                    "sensorId, "
                    "w.start as timestamp, "
                    "doubles(projectGuid, sensorId, timestamp, doubles, strings, integers, labels) as doubles,"
                    "strings(projectGuid, sensorId, timestamp, doubles, strings, integers, labels) as strings,"
                    "integers(projectGuid, sensorId, timestamp, doubles, strings, integers, labels) as integers,"
                    "labels(projectGuid, sensorId, timestamp, doubles, strings, integers, labels) as labels") \
            .window(window_slide)

        # Insert the result into the refit_sensor_data table
        windowed_table.execute_insert('refit_sensor_data')

        self.env.execute("CDL IoT - Feature Extraction")

RefitFeatureEnrichment().run_udf()
