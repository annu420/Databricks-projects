-- Databricks notebook source
-- MAGIC %md
-- MAGIC #MOVIE RECOMMENDATION USING GENRES

-- COMMAND ----------

-- CALLING OUR DATASET FOR STUDYING 
SELECT * FROM movies_3_csv

-- COMMAND ----------

-- SELECTING DESIRED COLUMNS
select title , popularity from movies_3_csv

-- COMMAND ----------

-- CREATING TEMP VIEW TO FILTER THE DATASET
CREATE or replace temp view temp_title as
SELECT title, explode(split(genres, ' ')) AS genres_array
FROM movies_3_csv ORDER BY genres_array

-- COMMAND ----------

-- TEMP VIEW TO FILTER OUT SPECIFIC TITLES WITH CLEANED GENRES 
create or replace temp view title_list as
select title from temp_title where genres_array in (select alias2.genres_array from (
select genres_array, count(*) as c from
temp_title group by genres_array) as alias2 where alias2.c > 3 )

-- COMMAND ----------

-- FINAL CLEANED DATASET FOR MAKING PIVOTED TABLE LATER ON
create or replace table movies_cleaned as 
select * , row_number() over(order by title) as rn1 from 
(select m.index, m.title, m.popularity, m.genres, m.cast, m.director,m.homepage ,m.overview ,row_number() over (partition by m.title order by m.title) as rn from movies_3_csv m join title_list tl on tl.title = m.title) as clean where clean.rn = 1 order by clean.title

-- COMMAND ----------

CREATE or replace temp view exploded_table as
SELECT rn1, explode(split(genres, ' ')) AS genres_array
FROM movies_cleaned ORDER BY genres_array;

select * from exploded_table

-- COMMAND ----------

create or replace temp view genres_list as
select genres_array, count(*) as mov_count from 
(SELECT rn1, explode(split(genres, ' ')) AS genres_array
FROM movies_cleaned ORDER BY genres_array) as a group by a.genres_array order by a.genres_array;

select * from genres_list

-- COMMAND ----------

-- ASSIGNING INTEGER VALUES TO EACH GENRES
create or replace temp view diamond_table as
select genres_array, mov_count,row_number() over (order by genres_array) as genres_value from genres_list where mov_count > 4;

select * from diamond_table

-- COMMAND ----------

-- RELATING INDEX VALUES OF THEIR CORRESPONDING TITLES TO THEIR RESPECTIVE GENRES
CREATE OR REPLACE TEMP VIEW final_table as
SELECT rn1, genres_array
FROM exploded_table where genres_array in (select genres_array from genres_list where mov_count > 4) ORDER BY genres_array;

select * from final_table

-- COMMAND ----------

-- COMBINING TWO TABLES IN ORDER TO ASSIGN INTEGER VALUES TO GENRES WITH RESPECT TO THEIR TITLES
create or replace temp view plat_table as
select f.rn1,f.genres_array, d.genres_value from final_table f join diamond_table d on f.genres_array = d.genres_array;

select * from plat_table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # CREATING DATAFRAME OF THE ABOVE TABLE IN ORDER TO PIVOT IT 
-- MAGIC import numpy as np
-- MAGIC df = spark.table("plat_table")
-- MAGIC # display(df)
-- MAGIC df_panda= df.toPandas()
-- MAGIC df_panda['genres_value'] = df_panda['genres_value'].astype(np.int64)
-- MAGIC df_panda['genres_array'] = df_panda['genres_array'].astype('string')
-- MAGIC df_panda['rn1'] = df_panda['rn1'].astype(np.int64)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # PIVOTING THE DATAFRAME 
-- MAGIC pivoted_table = df_panda.pivot(index='rn1', columns= "genres_array", values= "genres_value").fillna(0)
-- MAGIC display(pivoted_table)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from fuzzywuzzy import process
-- MAGIC def movie_recommender(movie_name,Data,n):
-- MAGIC     idx= process.extractOne(movie_name,df_pre_pa['title'])[2]
-- MAGIC     print("movie selected: " ,df_pre_pa['title'][idx], "Index: ",idx)
-- MAGIC     print("Movie recommendation......")
-- MAGIC     Distance, indices = model.kneighbors(Data[idx],n_neighbors = n)
-- MAGIC     # print(Distance, indices)
-- MAGIC     rec_movies={"indices": [i for i in indices], "movies":[df_pre_pa['title'][i].where(i!=idx) for i in indices]}
-- MAGIC     for i in indices:
-- MAGIC         print(df_pre_pa['title'][i].where(i!=idx))
-- MAGIC     #     rec_movies.append(df['title'][i].where(i!=idx))
-- MAGIC     dataframe=pd.DataFrame(rec_movies, columns = ['indices', 'movies'])
-- MAGIC     return(dataframe)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text(name="input_movie", defaultValue="Avatar", label="Type movie name")
-- MAGIC input_movie = dbutils.widgets.get("input_movie")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # IMPORTING NECESSARY LIBRARIES
-- MAGIC from sklearn.neighbors import NearestNeighbors
-- MAGIC from scipy.sparse import csr_matrix
-- MAGIC import pyspark.pandas as ps
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC # CREATING DATAFRAME OF OUR CLEANED TABLE TO INSERT IT INTO FUNCTION
-- MAGIC df_pre = spark.table("movies_cleaned")
-- MAGIC df_pre_pa = df_pre.toPandas()
-- MAGIC
-- MAGIC df = spark.table("plat_table")
-- MAGIC
-- MAGIC df_panda= df.toPandas()
-- MAGIC
-- MAGIC
-- MAGIC pivoted_table = df_panda.pivot(index='rn1', columns= "genres_array", values= "genres_value").fillna(0)
-- MAGIC
-- MAGIC
-- MAGIC # CONVERTING THE DATASET INTO SPARSE MATRIX
-- MAGIC mat_table= csr_matrix(pivoted_table.values)
-- MAGIC
-- MAGIC # TRAINING OR MODEL BY GIVING THE SPARSE MATRIX TABLE
-- MAGIC model= NearestNeighbors(metric= "euclidean", algorithm="brute", n_neighbors=20)
-- MAGIC model.fit(mat_table)
-- MAGIC
-- MAGIC
-- MAGIC rec_mov_frame= movie_recommender(input_movie, mat_table, 10)
-- MAGIC
-- MAGIC # CONVERTING OUR OUTPUT INTO SPARK DATAFRAME FROM PANDAS DATAFRAME TO DISPLAY ON TYHE DASHBOARD
-- MAGIC rec_mov_frame['movies']=rec_mov_frame['movies'].astype("string")
-- MAGIC
-- MAGIC rec_spark_frame = ps.DataFrame(rec_mov_frame).to_spark()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(rec_spark_frame)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rec_spark_frame.createOrReplaceTempView("temp_frame")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATING TABLES AND VISUALIZATION TO DISPLAY IT ON DASHSBOARD
-- MAGIC

-- COMMAND ----------

select title, genres, homepage from movies_cleaned  where rn1 -1 in (select explode(indices) from temp_frame)

-- COMMAND ----------

select title, popularity from movies_cleaned  where rn1 -1 in (select explode(indices) from temp_frame)

-- COMMAND ----------

select title, cast, director from movies_cleaned  where rn1 -1 in (select explode(indices) from temp_frame)

-- COMMAND ----------

select overview from movies_cleaned  where rn1 -1 in (select explode(indices) from temp_frame)
