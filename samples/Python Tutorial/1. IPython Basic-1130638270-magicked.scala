// Databricks notebook source
// DBTITLE 1, Introduction
// MAGIC %md
// MAGIC 
// MAGIC IPython is more powerful than the default python interpreter with extra functionality. You can use IPython with Python2 or Python3 which depends on which python you set in `zeppelin.python`. This tutorial will teach you how to use IPython and what kind of fancy feature IPython provides for you. 
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %md
// MAGIC 
// MAGIC # Prerequisite
// MAGIC 
// MAGIC First you need to use Zeppelin 0.8.0 or afterwards. You need to install the following 2 packages to make IPython work in Zeppelin.
// MAGIC 
// MAGIC * jupyter `pip install jupyter`
// MAGIC * grpcio `pip install grpcio`
// MAGIC * protobuf `pip install protobuf`
// MAGIC 
// MAGIC If you have anaconda installed, then you just need to install `grpcio` and `protobuf` as Jupyter is already included in anaconda.
// MAGIC 
// MAGIC 
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %md
// MAGIC 
// MAGIC # How to use IPython
// MAGIC 
// MAGIC After you get the above prerequisites installed, you can use IPython in Zeppelin via `%python.ipython` or `%spark.ipyspark`. If you want to make IPython as your default interpreter via `%python` or `%spark.pyspark`, then you can configure `zeppelin.python.useIPython` as `true` in python's interpreter setting or `zeppelin.pyspark.useIPython` as `true` in spark's interpreter setting. So that Zeppelin will use IPython as the default python interpreter, and will fall back to the old python interpreter if the IPython prerequisite is not met.
// MAGIC 
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %md
// MAGIC 
// MAGIC # IPython Help
// MAGIC 
// MAGIC There're 2 approaches to for getting python help in IPython. 
// MAGIC * Use `?` after the python object,
// MAGIC * Use `help` function
// COMMAND ----------

// DBTITLE 1, Get Python Help (1)
// MAGIC %python
// MAGIC %python.ipython
// MAGIC 
// MAGIC import sys
// MAGIC 
// MAGIC sys?
// COMMAND ----------

// DBTITLE 1, Get Python Help (2)
// MAGIC %python
// MAGIC %python.ipython
// MAGIC 
// MAGIC import sys
// MAGIC 
// MAGIC help(sys)
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %md
// MAGIC 
// MAGIC # IPython magic function
// MAGIC 
// MAGIC All the IPython magic functions are avalible in Zeppelin, here's one example of `%timeit`, for the complete IPython magic functions, you can check the [link](http://ipython.readthedocs.io/en/stable/interactive/magics.html) here.
// MAGIC 
// MAGIC 
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %python
// MAGIC %python.ipython
// MAGIC 
// MAGIC %timeit range(1000)
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %md
// MAGIC 
// MAGIC # Tab completion
// MAGIC 
// MAGIC Tab completion, especially for attributes, is a convenient way to explore the structure of any object you’re dealing with. Simply type `object_name.<TAB>` to view the object’s attributes. See the following screenshot of how tab completion works in IPython Interpreter.
// MAGIC ![alt text](https://user-images.githubusercontent.com/164491/34858941-3f28105a-f78e-11e7-8341-2fbfd306ba5b.gif "Logo Title Text 1")
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC 
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %md
// MAGIC 
// MAGIC 
// MAGIC # Visualization
// MAGIC 
// MAGIC One big advangae of notebook is that you can inline visualize with your code in notebook. There're many awesome visualization libraries in Python, you can use most of them in Zeppelin.
// MAGIC Refer this `IPython Visualization Tutorial` in Zeppelin for how to use Python visualization libraries in Apache Zeppelin.
// MAGIC 
// MAGIC 
// MAGIC 
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %md
// MAGIC 
// MAGIC # Use ZeppelinContext 
// MAGIC 
// MAGIC `ZeppelinContext` is a utlity class which provide the following features
// MAGIC 
// MAGIC * Dynamic forms
// MAGIC * Show DataFrame via builtin visualization
// MAGIC 
// MAGIC 
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %python
// MAGIC %python.ipython
// MAGIC 
// MAGIC z.input(name='my_name', defaultValue='hello')
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %python
// MAGIC %python.ipython
// MAGIC 
// MAGIC import pandas as pd
// MAGIC df = pd.DataFrame({'name':['a','b','c'], 'count':[12,24,18]})
// MAGIC z.show(df)
// COMMAND ----------

// DBTITLE 1, 
// MAGIC %python
// MAGIC %python.ipython
// MAGIC 
// COMMAND ----------

