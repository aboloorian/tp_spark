from pyspark.sql.functions import desc, count


def departement_count(df):
    return df.groupBy("departement") \
             .agg(count("*").alias("nb_people")) \
             .orderBy(desc("nb_people"), "departement")
