# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.types as T
from DataUtil import getDeltaDateKey
from calculate_model import CalculateModel

calculate_model = CalculateModel('resnet_models_pb/')


def do_predict_fuc(row):
    """模型加载和预测函数"""
    # row sp_id name upc url dt
    row_dict = row.asDict(True)
    # 将一行变为dict对象
    first_pic_url = row_dict["first_pic"]
    # 取first_pic数据
    try:
        embedding = calculate_model.predict(first_pic_url)
    except:
        embedding = [0.0] * 2048
        print('wgiwegw')
    # 计算结果
    row_dict['embedding'] = embedding
    # print('first_url: ', first_pic_url)
    # 将结果返回
    # row sp_id name upc url dt embedding
    return Row(**row_dict)


def get_predict_category(spark, spu_data):
    """数据分发，预测结果汇总建表"""
    schema = T.StructType([
        T.StructField("sp_id", T.LongType(), False),
        T.StructField("upc_code", T.StringType(), False),
        T.StructField("upc_product_name", T.StringType(), False),
        T.StructField("upc_pic", T.StringType(), False),
        T.StructField("first_category_id", T.LongType(), False),
        T.StructField("first_category_name", T.StringType(), False),
        T.StructField("second_category_id", T.LongType(), False),
        T.StructField("second_category_name", T.StringType(), False),
        T.StructField("third_category_id", T.LongType(), False),
        T.StructField("third_category_name", T.StringType(), False),
        T.StructField("embedding", T.ArrayType(elementType=T.FloatType()), False),
        T.StructField("dt", T.StringType(), False)
    ])
    # 提前预设 数据结构
    rdd1 = spu_data.rdd.repartition(3000).map(do_predict_fuc)
    predict_res = spark.createDataFrame(
        rdd1.map(
            lambda row: Row(
                row.sp_id,
                row.upc_code,
                row.product_name,
                row.first_pic,
                row.first_category_id,
                row.first_category_name,
                row.second_category_id,
                row.second_category_name,
                row.third_category_id,
                row.third_category_name,
                row.embedding,
                row.dt
            )
        ), schema
        # 对spu_data执行do_predict_fuc映射得到的结果成为一个row然后填充至预设的schema中
    )
    return predict_res


def get_full_sp_data(spark, dt):
    """从标品库中获取前一日的所有标品"""
    sql = '''
        select sp_id, 
               product_name,
              first_category_id,
              first_category_name,
              second_category_id,
              second_category_name,
              third_category_id,
              third_category_name,
               upc_code, 
               split(pic, ",")[0] as first_pic, 
               dt
            from mart_lingshou.dim_prod_standard_product_s_snapshot
             where dt = '{}'
               and is_valid=1 -- 有效数据
               and length(pic) > 0
               and product_name is not null
               and length(product_name) > 0
               and third_category_name is not null
               and length(third_category_name) > 0
               and second_category_name is not null
               and length(second_category_name) > 0
               and first_category_name is not null
               and length(first_category_name) > 0
        '''.format(dt)
    tmp = spark.sql(sql)
    tmp.printSchema()
    print('获得了所有的标品库数据')
    return tmp


def save_res_into_hive(spark, dt, result_df, hive_path, re_creat_table=False):
    """将结果数据写入 Hive 表"""
    # 重建数据表，支持字段变化
    print('开始写表')
    if re_creat_table:
        # 删除
        spark.sql('DROP TABLE IF EXISTS %s' % hive_path)
        # 字段类型
        schemaList = []
        for x in result_df.dtypes:
            if x[0] == 'dt':
                continue
            schemaList.append(x[0] + ' ' + x[1])
        colNamesAndType = ','.join(schemaList)

        # 建表 SQL
        createSQL = '''
                    CREATE TABLE IF NOT EXISTS %s (%s)
                    PARTITIONED BY (%s string)
                    STORED AS ORC
                ''' % (hive_path, colNamesAndType, 'dt')
        spark.sql(createSQL)

    # 写出数据
    spark.sql('''
                    alter table {0}
                    drop if exists partition(dt={1})
                '''.format(hive_path, dt))

    # result_df.write.insertInto(hive_path)
    result_df.repartition(3000).write.mode('overwrite').insertInto(hive_path)
    print("dt = {} \t success save into {}".format(dt, hive_path))


def run(dt, predict_type):
    spark = SparkSession \
        .builder \
        .appName("image_embedding_full_sp -- by wy") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    if predict_type in ('full_sp_prod', 'increment_sp_prod'):
        hive_path = "mart_shangou_alg.dim_prod_standard_product_first_image_embedding_by_resnet50"
    else:
        hive_path = ""
    sp_data = get_full_sp_data(spark, dt)
    # 获得了sp_data

    embedding = get_predict_category(spark, sp_data)

    save_res_into_hive(spark, dt, embedding, hive_path, re_creat_table=False)

