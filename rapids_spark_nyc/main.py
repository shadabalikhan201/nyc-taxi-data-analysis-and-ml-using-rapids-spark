import sys
#from com.isomode.rapids_spark_nyc.eda.Exploratory_Data_Analysis import Exploratory_Data_Analysis
from rapids_spark_nyc.spark_session.Spark import Spark


def main():

    project_home = sys.argv[1]
    print(project_home)

    glow_spark_session = Spark.get_spark_session(project_home)
    '''try:
        seq_df = File_Reader().read(glow_spark_session, project_home)
        seq_df.createOrReplaceTempView('seq_df')
        nucleotide_count_df = Exploratory_Data_Analysis().get_per_nucleotide_quality(seq_df)
        nucleotide_count_df.createOrReplaceTempView('nucleotide_count_df')
        Dashboard(project_home).get_dashboard_home('NomeDotBio', list([['fastq_sequence', 'seq_df'],['nucleotide_count', 'nucleotide_count_df']]))

    except NomeReadException:
        raise NomeReadException

    finally:
        Spark.destroy_spark_session()'''


if __name__ == '__main__':
    main()
