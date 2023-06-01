from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Over
from pyflink.table import expressions as expr

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
        from feature_extractors.functions import doubles, strings, integers, labels
        self.table_env.register_function("doubles", doubles)
        self.table_env.register_function("strings", strings)
        self.table_env.register_function("integers", integers)
        self.table_env.register_function("labels", labels)

        # consider the last 10 rows
        windowed_table = self.table_env.from_path('refit_raw_sensor_data') \
            .select("projectGuid, "
                    "sensorId, "
                    "timestamp, "
                    "doubles(projectGuid, sensorId, timestamp, doubles, strings, integers, labels) "
                    "   OVER (PARTITION BY projectGuid, sensorId "
                    "         ORDER BY timestamp.rowsBetween(-9, 0)) as doubles, "
                    "strings(projectGuid, sensorId, timestamp, doubles, strings, integers, labels) "
                    "   OVER (PARTITION BY projectGuid, sensorId "
                    "         ORDER BY timestamp.rowsBetween(-9, 0)) as strings, "
                    "integers(projectGuid, sensorId, timestamp, doubles, strings, integers, labels) "
                    "   OVER (PARTITION BY projectGuid, sensorId "
                    "         ORDER BY timestamp.rowsBetween(-9, 0)) as integers, "
                    "labels(projectGuid, sensorId, timestamp, doubles, strings, integers, labels) "
                    "   OVER (PARTITION BY projectGuid, sensorId "
                    "         ORDER BY timestamp.rowsBetween(-9, 0)) as labels")

        windowed_table.execute_insert('refit_sensor_data')
        self.env.execute("CDL IoT - Feature Extraction")

RefitFeatureEnrichment().run_udf()
