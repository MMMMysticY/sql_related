# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.types as T
import pyspark.sql.functions as F
from DataUtil import getDeltaDateKey
from calculate_model import CalculateModel

calculate_model = CalculateModel('resnet_models_pb/')


def do_predict_fuc(row):
    """模型加载和预测函数"""
    # row sp_id name upc url dt
    row_dict = row.asDict(True)
    # 将一行变为dict对象
    sp_embedding = row_dict["sp_embedding"]
    product_pic = row_dict["product_pic"]
    # 取first_pic数据
    try:
        similarity = calculate_model.predict(product_pic, sp_embedding)
    except:
        similarity = 0.0
        print('wgiwegw')
    # 计算结果
    row_dict['score'] = similarity
    print('product_pic: ', product_pic)
    # 将结果返回
    return Row(**row_dict)


def get_predict_category(spark, spu_data):
    """数据分发，预测结果汇总建表"""
    schema = T.StructType([
        T.StructField("sp_id", T.LongType(), False),
        T.StructField("upc_code", T.StringType(), False),
        T.StructField("upc_product_name", T.StringType(), False),
        T.StructField("upc_pic", T.StringType(), False),
        T.StructField("product_spu_id", T.LongType(), False),
        T.StructField("product_spu_name", T.StringType(), True),
        T.StructField("product_pic", T.StringType(), False),
        T.StructField("first_category_id", T.LongType(), True),
        T.StructField("first_category_name", T.StringType(), True),
        T.StructField("second_category_id", T.LongType(), True),
        T.StructField("second_category_name", T.StringType(), True),
        T.StructField("third_category_id", T.LongType(), True),
        T.StructField("third_category_name", T.StringType(), True),
        T.StructField("poi_id", T.LongType(), True),
        T.StructField("poi_name", T.StringType(), True),
        T.StructField("poi_first_category_id", T.LongType(), True),
        T.StructField("poi_first_category_name", T.StringType(), True),
        T.StructField("score", T.StringType(), False)
    ])
    # 提前预设 数据结构
    rdd1 = spu_data.rdd.repartition(3000).map(do_predict_fuc)
    predict_res = spark.createDataFrame(
        rdd1.map(
            lambda row: Row(
                row.sp_id,
                row.upc_code,
                row.upc_product_name,
                row.upc_pic,
                row.product_spu_id,
                row.product_spu_name,
                row.product_pic,
                row.first_category_id,
                row.first_category_name,
                row.second_category_id,
                row.second_category_name,
                row.third_category_id,
                row.third_category_name,
                row.poi_id,
                row.poi_name,
                row.poi_first_category_id,
                row.poi_first_category_name,
                row.score,
            )
        ), schema
        # 对spu_data执行do_predict_fuc映射得到的结果成为一个row然后填充至预设的schema中
    )
    return predict_res


def get_increment_spu_data(spark, dt):
    """ 获取需要预测的增量 spu 去重名称，spu 增量是 dt 比前一天新增的商品 """
    yesterday = getDeltaDateKey(dt, 1)
    sql = '''
        select 	
        s_p.sp_id as sp_id,						-- 标品id
		s_p.upc_code as upc_code,				-- upc码
        upc_product_name,						-- 标品名称
        upc_pic,								-- 标品图片
        product_spu_id,							-- 商品id
        product_spu_name,						-- 商品标题
        product_pic,							-- 商品图片
        first_category_id,						-- 后台一级类目id
        first_category_name,					-- 后台一级类目
        second_category_id,						-- 后台二级类目id
        second_category_name,					-- 后台二级类目
        third_category_id,						-- 后台三级类目id
        third_category_name,					-- 后台三级类目
        poi_id,									-- 商家 id
        poi_name,								-- 商家名称
        poi_first_category_id,					-- 商家一级经营品类 id
        poi_first_category_name,				-- 商家一级经营类名称
        embedding as sp_embedding				-- sp embedding
from

(select
		sp_id,					-- 标品id
        upc_code,				-- upc码
        upc_product_name,		-- 标品名称
        upc_pic,				-- 标品图片
        first_category_id,		-- 后台一级类目id
        first_category_name,	-- 后台一级类目
        second_category_id,		-- 后台二级类目id
        second_category_name,	-- 后台二级类目
        third_category_id,		-- 后台三级类目id
        third_category_name,	-- 后台三级类目
        embedding				-- 标品embedding
 	from mart_shangou_alg.dim_prod_standard_product_first_image_embedding_by_resnet50
 	where dt='{0}'
  )s_p
  inner join
  (SELECT 	sp_id, 					-- sp_id 标品id
 				upc_code, 				-- upc码
 				a.product_spu_id as product_spu_id, 		-- 商品id
 				product_spu_name, 		-- 商品标题
 				product_pic, 			-- 商品图片
 				poi_id, 				-- 商家 id
 				poi_name, 				-- 商家名称
 				poi_first_category_id, 	-- 商家一级经营品类 id
 				poi_first_category_name -- 商家一级经营类名称

  		from (
        		select
						sp_id,
       					upc_code,
       					today.product_spu_id as product_spu_id,
       					product_spu_name,
       					poi_id,
       					poi_name,
       					poi_first_category_id,
       					poi_first_category_name
  				from
  				(		SELECT
    							*
          				FROM mart_shangou_alg.topic_prod_upc_match_sku_dd
         				WHERE dt='{0}' -- 时间是今天

           				AND product_spu_id is not null -- product_spu_id不能是null
       							)today
  							left outer join
        				(
        				select product_spu_id
          				FROM mart_shangou_alg.topic_prod_upc_match_sku_dd
         				where dt='{1}'
           				and product_spu_id is not null
       					)yestarday
    				on today.product_spu_id = yestarday.product_spu_id
 				where yestarday.product_spu_id is null
                     	AND today.category_type in (1,4)-- 去除掉不需要的类别
           				AND today.match_flag in (2,3)-- 选择不完全匹配和完全不匹配
       		)a
  		join (
        		-- 商品信息
 				select product_spu_id, 							-- 商品spu id
 				split(product_picture, ",")[0] as product_pic 	-- 从product_picture取出商品首图
          		from mart_lingshou.dim_prod_product_sku_s_snapshot
         		where dt='{0}'
           		and is_valid=1
           		and is_delete=0
           		and product_picture != ''
       		) b
    		on a.product_spu_id=b.product_spu_id 				-- 在spu_id上join

  )pro
  on s_p.upc_code=pro.upc_code
  where pro.product_pic is not null
  limit 3000
  '''.format(dt, yesterday)
    tmp = spark.sql(sql)
    tmp.printSchema()
    return tmp


def merge_increment_spu(spark, dt, hive_path):
    """ 将存量商品与增量 spu 的预测结果合并 """
    yesterday = getDeltaDateKey(dt, 1)
    sql = '''
        select 
                sp_id,
                upc_code,
                upc_product_name,
                upc_pic,
                product_spu_id,
                product_spu_name,
                product_pic,
                first_category_id,
                first_category_name,
                second_category_id,
                second_category_name,
                third_category_id,
                third_category_name,
                poi_id,
                poi_name,
                poi_first_category_id,
                poi_first_category_name,
                score
          from {0}
         where dt='{1}'

        union 

        select sp_id,
                upc_code,
                upc_product_name,
                upc_pic,
                product_spu_id,
                product_spu_name,
                product_pic,
                first_category_id,
                first_category_name,
                second_category_id,
                second_category_name,
                third_category_id,
                third_category_name,
                poi_id,
                poi_name,
                poi_first_category_id,
                poi_first_category_name,
                score
          from increment_spu_table
    '''.format(hive_path, yesterday)
    tmp = spark.sql(sql)
    tmp.printSchema()
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
        .appName("image_similarity_of_sp_and_sku -- by wy") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    if predict_type in ('full_sp_prod', 'increment_sp_prod'):
        hive_path = "mart_shangou_alg.dim_prod_standard_product_sku_image_similarity_by_resnet50"
    else:
        hive_path = ""
    #
    print('开始！！！')
    spu_data = get_increment_spu_data(spark, dt)
    print('获得了spu_data！')
    # 获得了spu_data 大概360w条 表示的是相对于前一天的增量
    cols = ["sp_id", "upc_code", "upc_product_name", "upc_pic", "product_spu_id", "product_spu_name",
            "product_pic", "first_category_id", "first_category_name", "second_category_id",
            "second_category_name", "third_category_id", "third_category_name", "poi_id",
            "poi_name", "poi_first_category_id", "poi_first_category_name", "score"]
    predict_res = get_predict_category(spark, spu_data)
    print('计算出了score')
    # 通过product_pic和sp_embedding计算出了score
    #predict_res.select(*cols).createOrReplaceTempView('increment_spu_table')
    #print('构建了临时表')
    # 构建临时表
    #predict_res = merge_increment_spu(spark, dt, hive_path)
    #print('merge结束')
    # 和前一天的进行merge
    res_df = predict_res.withColumn('dt', F.lit(dt))
    print('设置了时间')
    res_df.printSchema()
    # 时间设置成dt
    print('写表')
    save_res_into_hive(spark, dt, res_df, hive_path, re_creat_table=True)
