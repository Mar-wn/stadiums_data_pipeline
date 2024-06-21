{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "22e0bd8b",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "24/06/21 17:37:55 WARN Utils: Your hostname, marwen resolves to a loopback address: 127.0.1.1; using 192.168.1.4 instead (on interface wlp3s0)\n",
      "24/06/21 17:37:55 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address\n",
      "Setting default log level to \"WARN\".\n",
      "To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).\n",
      "24/06/21 17:37:59 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable\n"
     ]
    }
   ],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "\n",
    "spark = SparkSession.builder.appName('stads').getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "9aaa7a7b",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "\n",
       "            <div>\n",
       "                <p><b>SparkSession - in-memory</b></p>\n",
       "                \n",
       "        <div>\n",
       "            <p><b>SparkContext</b></p>\n",
       "\n",
       "            <p><a href=\"http://192.168.1.4:4040\">Spark UI</a></p>\n",
       "\n",
       "            <dl>\n",
       "              <dt>Version</dt>\n",
       "                <dd><code>v3.5.1</code></dd>\n",
       "              <dt>Master</dt>\n",
       "                <dd><code>local[*]</code></dd>\n",
       "              <dt>AppName</dt>\n",
       "                <dd><code>stads</code></dd>\n",
       "            </dl>\n",
       "        </div>\n",
       "        \n",
       "            </div>\n",
       "        "
      ],
      "text/plain": [
       "<pyspark.sql.session.SparkSession at 0x7ff205a8fa60>"
      ]
     },
     "execution_count": 2,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "fcedb8ae",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "df = spark.read.csv('stads.csv', inferSchema= True, header= True, multiLine= True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "f42940e4",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "pyspark.sql.dataframe.DataFrame"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "type(df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "1f49cd1f",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "DataFrame[Stadium: string, Seating capacity: string, Region: string, Country: string, City: string, Images: string, Home team(s): string]"
      ]
     },
     "execution_count": 5,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "531a29fb",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+----------------+----------------+----------------+--------------------+------+--------------------+\n",
      "|             Stadium|Seating capacity|          Region|         Country|                City|Images|        Home team(s)|\n",
      "+--------------------+----------------+----------------+----------------+--------------------+------+--------------------+\n",
      "|Rungrado 1st of M...|      114,000[1]|       East Asia|     North Korea|         Pyongyang\\n|  NULL|Korea DPR nationa...|\n",
      "|    Michigan Stadium|      107,601[2]|   North America|   United States|Ann Arbor, Michig...|    \\n|Michigan Wolverin...|\n",
      "|        Ohio Stadium|      102,780[3]|   North America|   United States|      Columbus, Ohio|  NULL|Ohio State Buckey...|\n",
      "|Melbourne Cricket...|      100,024[4]|         Oceania|       Australia| Melbourne, Victoria|  NULL|Australia nationa...|\n",
      "|          Camp Nou ♦|       99,354[5]|          Europe|           Spain|Barcelona, Catalo...|  NULL|      FC Barcelona\\n|\n",
      "|    Estadio Azteca ♦|       95,500[6]|   North America|          Mexico|       Mexico City\\n|  NULL|Club América, Cru...|\n",
      "|       FNB Stadium ♦|       94,736[7]|          Africa|    South Africa|Johannesburg, Gau...|  NULL|South Africa nati...|\n",
      "|New Administrativ...|       93,940[8]|          Africa|           Egypt|New Administrativ...|  NULL|Egypt national fo...|\n",
      "|   Rose Bowl Stadium|       92,800[9]|   North America|   United States|Pasadena, Califor...|  NULL|       UCLA Bruins\\n|\n",
      "| Cotton Bowl Stadium|      92,100[10]|   North America|   United States|       Dallas, Texas|  NULL|                  \\n|\n",
      "| Wembley Stadium ♦\\n|    90,000[11]\\n|        Europe\\n|       England\\n|   London, England\\n|    \\n|England national ...|\n",
      "|  Lusail Stadium ♦\\n|    88,966[12]\\n|     West Asia\\n|         Qatar\\n|            Lusail\\n|    \\n|Qatar national fo...|\n",
      "|Bukit Jalil Natio...|          87,411|  Southeast Asia|        Malaysia|      Kuala Lumpur\\n|  NULL|Malaysia national...|\n",
      "|Borg el-Arab Stad...|      86,000[14]|          Africa|           Egypt|        Alexandria\\n|  NULL|Egypt national fo...|\n",
      "|Estadio Santiago ...|      85,000[15]|          Europe|           Spain|            Madrid\\n|  NULL|       Real Madrid\\n|\n",
      "|Estadio Mâs Monum...|          84,567|   South America|       Argentina|      Buenos Aires\\n|  NULL|Argentina nationa...|\n",
      "|   Stadium Australia|          83,500|         Oceania|     Australia\\n|Sydney, New South...|  NULL|South Sydney Rabb...|\n",
      "|     MetLife Stadium|          82,500|   North America| United States\\n|East Rutherford, ...|  NULL|New York Giants, ...|\n",
      "|        Croke Park ♦|      82,300[16]|          Europe|         Ireland|            Dublin\\n|  NULL|Gaelic Athletic A...|\n",
      "|Jakarta Internati...|        82,000\\n|Southeast Asia\\n|     Indonesia\\n|           Jakarta\\n|    \\n|Persija Jakarta, ...|\n",
      "+--------------------+----------------+----------------+----------------+--------------------+------+--------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "97e4b0cc",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- Stadium: string (nullable = true)\n",
      " |-- Seating capacity: string (nullable = true)\n",
      " |-- Region: string (nullable = true)\n",
      " |-- Country: string (nullable = true)\n",
      " |-- City: string (nullable = true)\n",
      " |-- Images: string (nullable = true)\n",
      " |-- Home team(s): string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "e17657b7",
   "metadata": {},
   "outputs": [],
   "source": [
    "# tranformation functions\n",
    "\n",
    "def clean_text(text):\n",
    "    \n",
    "    import re\n",
    "    \n",
    "    # replace newline characters with an empty string\n",
    "    if text:\n",
    "        text = text.replace('\\n', '')\n",
    "    \n",
    "    # replace the '♦' character with an empty string\n",
    "    if text:\n",
    "        text = text.replace('♦', '')\n",
    "    \n",
    "    # remove leading and trailing whitespace\n",
    "    if text:\n",
    "        text = re.sub(r'^\\s+|\\s+$', '', text)\n",
    "    \n",
    "    return text\n",
    "\n",
    "\n",
    "def clean_integer(string):\n",
    "    \n",
    "    import re\n",
    "\n",
    "    string = re.sub(r'\\[.*?\\]', '', string)\n",
    "        \n",
    "    if string:\n",
    "        \n",
    "        string = string.replace(',', '') \n",
    "\n",
    "    return string"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "4d43a8c7",
   "metadata": {},
   "outputs": [],
   "source": [
    "# create udfs\n",
    "\n",
    "from pyspark.sql.functions import udf\n",
    "\n",
    "from pyspark.sql.types import StringType\n",
    "\n",
    "clean_text_udf = udf(clean_text, StringType())\n",
    "\n",
    "clean_integer_udf = udf(clean_integer, StringType())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "fb5dc4b0",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "[Stage 3:>                                                          (0 + 1) / 1]\r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+----------------+--------------+-------------+--------------------+------+--------------------+\n",
      "|             Stadium|Seating capacity|        Region|      Country|                City|Images|        Home team(s)|\n",
      "+--------------------+----------------+--------------+-------------+--------------------+------+--------------------+\n",
      "|Rungrado 1st of M...|          114000|     East Asia|  North Korea|           Pyongyang|  NULL|Korea DPR nationa...|\n",
      "|    Michigan Stadium|          107601| North America|United States| Ann Arbor, Michigan|    \\n|Michigan Wolverin...|\n",
      "|        Ohio Stadium|          102780| North America|United States|      Columbus, Ohio|  NULL|Ohio State Buckey...|\n",
      "|Melbourne Cricket...|          100024|       Oceania|    Australia| Melbourne, Victoria|  NULL|Australia nationa...|\n",
      "|            Camp Nou|           99354|        Europe|        Spain|Barcelona, Catalonia|  NULL|        FC Barcelona|\n",
      "|      Estadio Azteca|           95500| North America|       Mexico|         Mexico City|  NULL|Club América, Cru...|\n",
      "|         FNB Stadium|           94736|        Africa| South Africa|Johannesburg, Gau...|  NULL|South Africa nati...|\n",
      "|New Administrativ...|           93940|        Africa|        Egypt|New Administrativ...|  NULL|Egypt national fo...|\n",
      "|   Rose Bowl Stadium|           92800| North America|United States|Pasadena, California|  NULL|         UCLA Bruins|\n",
      "| Cotton Bowl Stadium|           92100| North America|United States|       Dallas, Texas|  NULL|                    |\n",
      "|     Wembley Stadium|           90000|        Europe|      England|     London, England|    \\n|England national ...|\n",
      "|      Lusail Stadium|           88966|     West Asia|        Qatar|              Lusail|    \\n|Qatar national fo...|\n",
      "|Bukit Jalil Natio...|           87411|Southeast Asia|     Malaysia|        Kuala Lumpur|  NULL|Malaysia national...|\n",
      "|Borg el-Arab Stadium|           86000|        Africa|        Egypt|          Alexandria|  NULL|Egypt national fo...|\n",
      "|Estadio Santiago ...|           85000|        Europe|        Spain|              Madrid|  NULL|         Real Madrid|\n",
      "|Estadio Mâs Monum...|           84567| South America|    Argentina|        Buenos Aires|  NULL|Argentina nationa...|\n",
      "|   Stadium Australia|           83500|       Oceania|    Australia|Sydney, New South...|  NULL|South Sydney Rabb...|\n",
      "|     MetLife Stadium|           82500| North America|United States|East Rutherford, ...|  NULL|New York Giants, ...|\n",
      "|          Croke Park|           82300|        Europe|      Ireland|              Dublin|  NULL|Gaelic Athletic A...|\n",
      "|Jakarta Internati...|           82000|Southeast Asia|    Indonesia|             Jakarta|    \\n|Persija Jakarta, ...|\n",
      "+--------------------+----------------+--------------+-------------+--------------------+------+--------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "# apply transformations to the df\n",
    "\n",
    "# for all columns except Images, clean text\n",
    "for column in df.columns:\n",
    "\n",
    "    if column != 'Images':\n",
    "    \n",
    "        df = df.withColumn(column, clean_text_udf(df[column]))\n",
    "\n",
    "# for Seating capacity column, apply integer specific transformations\n",
    "df = df.withColumn('Seating capacity', clean_integer_udf(df['Seating capacity']))\n",
    "\n",
    "df.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "f897670c",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------+----------------+--------------+-------------+--------------------+------+--------------------+\n",
      "|             Stadium|Seating capacity|        Region|      Country|                City|Images|        Home team(s)|\n",
      "+--------------------+----------------+--------------+-------------+--------------------+------+--------------------+\n",
      "|Rungrado 1st of M...|          114000|     East Asia|  North Korea|           Pyongyang|  NULL|Korea DPR nationa...|\n",
      "|    Michigan Stadium|          107601| North America|United States| Ann Arbor, Michigan|    \\n|Michigan Wolverin...|\n",
      "|        Ohio Stadium|          102780| North America|United States|      Columbus, Ohio|  NULL|Ohio State Buckey...|\n",
      "|Melbourne Cricket...|          100024|       Oceania|    Australia| Melbourne, Victoria|  NULL|Australia nationa...|\n",
      "|            Camp Nou|           99354|        Europe|        Spain|Barcelona, Catalonia|  NULL|        FC Barcelona|\n",
      "|      Estadio Azteca|           95500| North America|       Mexico|         Mexico City|  NULL|Club América, Cru...|\n",
      "|         FNB Stadium|           94736|        Africa| South Africa|Johannesburg, Gau...|  NULL|South Africa nati...|\n",
      "|New Administrativ...|           93940|        Africa|        Egypt|New Administrativ...|  NULL|Egypt national fo...|\n",
      "|   Rose Bowl Stadium|           92800| North America|United States|Pasadena, California|  NULL|         UCLA Bruins|\n",
      "| Cotton Bowl Stadium|           92100| North America|United States|       Dallas, Texas|  NULL|                    |\n",
      "|     Wembley Stadium|           90000|        Europe|      England|     London, England|    \\n|England national ...|\n",
      "|      Lusail Stadium|           88966|     West Asia|        Qatar|              Lusail|    \\n|Qatar national fo...|\n",
      "|Bukit Jalil Natio...|           87411|Southeast Asia|     Malaysia|        Kuala Lumpur|  NULL|Malaysia national...|\n",
      "|Borg el-Arab Stadium|           86000|        Africa|        Egypt|          Alexandria|  NULL|Egypt national fo...|\n",
      "|Estadio Santiago ...|           85000|        Europe|        Spain|              Madrid|  NULL|         Real Madrid|\n",
      "|Estadio Mâs Monum...|           84567| South America|    Argentina|        Buenos Aires|  NULL|Argentina nationa...|\n",
      "|   Stadium Australia|           83500|       Oceania|    Australia|Sydney, New South...|  NULL|South Sydney Rabb...|\n",
      "|     MetLife Stadium|           82500| North America|United States|East Rutherford, ...|  NULL|New York Giants, ...|\n",
      "|          Croke Park|           82300|        Europe|      Ireland|              Dublin|  NULL|Gaelic Athletic A...|\n",
      "|Jakarta Internati...|           82000|Southeast Asia|    Indonesia|             Jakarta|    \\n|Persija Jakarta, ...|\n",
      "+--------------------+----------------+--------------+-------------+--------------------+------+--------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# convert Seating capacity to integer\n",
    "from pyspark.sql.functions import col\n",
    "\n",
    "df = df.withColumn(\"Seating capacity\", col(\"Seating capacity\").cast('integer'))\n",
    "\n",
    "df.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "af35b0fe",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- Stadium: string (nullable = true)\n",
      " |-- Seating capacity: integer (nullable = true)\n",
      " |-- Region: string (nullable = true)\n",
      " |-- Country: string (nullable = true)\n",
      " |-- City: string (nullable = true)\n",
      " |-- Images: string (nullable = true)\n",
      " |-- Home team(s): string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "8d2d0cf0",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    }
   ],
   "source": [
    "df.write.csv('output', header= True)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python (stads)",
   "language": "python",
   "name": "stads"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
